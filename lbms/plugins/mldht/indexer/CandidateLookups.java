package lbms.plugins.mldht.indexer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


import lbms.plugins.mldht.indexer.MetaDataGatherer.FetchTask;
import lbms.plugins.mldht.indexer.assemblyline.AssemblyTask;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.ScrapeResponseHandler;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.tasks.TaskListener;

public class CandidateLookups implements AssemblyTask {
	
	AtomicInteger activeLookups = new AtomicInteger();
	BlockingQueue<TorrentDBEntry> fetchCandidates;
	BlockingQueue<TorrentDBEntry> scrapeCandidates;
	BlockingQueue<FetchTask> fetchTasks;
	BlockingQueue<BatchQuery> queries;
	
	MetaDataGatherer container;
	
	public CandidateLookups(MetaDataGatherer container, BlockingQueue<TorrentDBEntry> toFetch, BlockingQueue<TorrentDBEntry> toScrape, BlockingQueue<FetchTask> fetch, BlockingQueue<BatchQuery> queries) {
		this.container = container;
		this.fetchCandidates = toFetch;
		this.scrapeCandidates = toScrape;
		this.fetchTasks = fetch;
		this.queries = queries;
	}
	
	public boolean performTask() {
		if(activeLookups.get() >= container.getNumVirtualNodes() * MetaDataGatherer.LOOKUPS_PER_VIRTUAL_NODE || queries.remainingCapacity() == 0)
			return false;

		TorrentDBEntry entry = null;
		if(fetchTasks.remainingCapacity() > 0)
			entry = fetchCandidates.poll();
		if(entry == null) entry = scrapeCandidates.poll();
		if(entry == null)
			return false;

		final FetchTask task = new FetchTask();
		task.entry = entry;
		task.addresses = new ArrayList<PeerAddressDBItem>();
		task.hash = new Key(entry.info_hash);

		MetaDataGatherer.log("starting DHT lookup for "+task.hash);

		final int[] pendingLookups = new int[1];

		final ScrapeResponseHandler scrapeHandler = new ScrapeResponseHandler();

		task.scrapes = scrapeHandler;

		TaskListener lookupListener = new TaskListener() {
			public synchronized void finished(Task t) {
				int pending = --pendingLookups[0];
				if(pending >= 0)
				{
					if(task.entry.status == TorrentDBEntry.STATE_CURRENTLY_ATTEMPTING_TO_FETCH)
					{
						PeerLookupTask pt = (PeerLookupTask) t;
						task.addresses.addAll(pt.getReturnedItems());

					}

					MetaDataGatherer.log("one DHT lookup done for "+task.hash);
				}

				if(pending == 0)
				{
					MetaDataGatherer.log("all DHT lookups done for"+task.hash);
					activeLookups.decrementAndGet();
					scrapeHandler.process();

					if(task.entry.status == TorrentDBEntry.STATE_CURRENTLY_ATTEMPTING_TO_FETCH)
					{
						task.addresses.removeAll(Collections.singleton(null));
						if(task.addresses.size() > 0) {
							// trim to a limited amount of addresses to avoid 1 task being stuck for ages
							if(task.addresses.size() > MetaDataGatherer.MAX_ATTEMPTS_PER_INFOHASH)
								task.addresses.subList(MetaDataGatherer.MAX_ATTEMPTS_PER_INFOHASH, task.addresses.size()).clear();
							fetchTasks.add(task);
							MetaDataGatherer.log("added metadata task based on DHT for "+task.hash);
						} else {
							MetaDataGatherer.log("found no DHT entires for "+task.hash);
							queries.add(container.createFailedTask(task.hash));
						}

					} else {
						queries.add(new BatchQuery(task.hash) {
							public void run() {
								session.createQuery("update ihdata e set e.hitCount = 0, e.lastLookupTime = :time where e.info_hash  = :hash and e.status > 1")
								.setParameter("time", System.currentTimeMillis()/1000)
								.setParameter("hash", task.entry.info_hash)
								.executeUpdate();
								container.saveScrapes(session, task.scrapes, task.entry);
							}
						});
					}
				}
			}
		};



		for(DHTtype type : DHTtype.values())
		{
			DHT dht = DHT.getDHT(type);
			PeerLookupTask lookupTask = dht.createPeerLookup(task.entry.info_hash);
			if(lookupTask != null)
			{
				pendingLookups[0]++;

				lookupTask.setFastTerminate(true);
				lookupTask.setNoAnnounce(true);
				lookupTask.setLowPriority(false);
				lookupTask.addListener(lookupListener);
				lookupTask.setScrapeHandler(scrapeHandler);
				if(entry.status == TorrentDBEntry.STATE_CURRENTLY_ATTEMPTING_TO_FETCH)
					lookupTask.setInfo("Grabbing .torrent for "+task.hash);
				else
					lookupTask.setInfo("Scraping "+task.hash);
				lookupTask.setNoSeeds(false);
				dht.getTaskManager().addTask(lookupTask);
			}
		}

		if(pendingLookups[0] > 0)
		{
			activeLookups.incrementAndGet();
			return true;
		}

		// this usually shouldn't happen but sometimes no RPC server may be availble despite numVirtualNodes > 0

		queries.add(container.createFailedTask(task.hash));
		return false;
			
	}
	
}
