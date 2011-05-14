/*
 *    This file is part of mlDHT. 
 * 
 *    mlDHT is free software: you can redistribute it and/or modify 
 *    it under the terms of the GNU General Public License as published by 
 *    the Free Software Foundation, either version 2 of the License, or 
 *    (at your option) any later version. 
 * 
 *    mlDHT is distributed in the hope that it will be useful, 
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *    GNU General Public License for more details. 
 * 
 *    You should have received a copy of the GNU General Public License 
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>. 
 */
package lbms.plugins.mldht.indexer;

import java.io.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.gudy.azureus2.core3.util.BDecoder;
import org.gudy.azureus2.core3.util.BEncoder;
import org.hibernate.*;
import org.hibernate.criterion.*;



import lbms.plugins.mldht.indexer.MetaDataConnectionServer.IncomingConnectionHandler;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.InfohashChecker;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.MetaConnectionHandler;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.tasks.TaskListener;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import lbms.plugins.mldht.utlis.NIOConnectionManager;

public class MetaDataGatherer {
	
	public static final int MAX_ATTEMPTS_PER_INFOHASH = 50;
	public static final int LOOKUPS_PER_VIRTUAL_NODE = 3;
	public static final int PIVOT_EVERY_N_VIRTUAL_NODES = 3;
	public static final int MIN_PIVOTS = 32;
	public static final int MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE = 3;
	public static final int MAX_FINISHED_UPDATE_CHARGE = 100;
	public static final int MAX_LOOKUPS_PER_PIVOT = 9;

	ArrayList<Key> lookupPivotPoints = new ArrayList<Key>();
	AtomicInteger activeLookups = new AtomicInteger();
	AtomicInteger activeOutgoingConnections = new AtomicInteger();
	ConcurrentLinkedQueue<FetchTask> fetchTasks = new ConcurrentLinkedQueue<FetchTask>();
	
	ConcurrentLinkedQueue<SessionRunnable> toFinish = new ConcurrentLinkedQueue<MetaDataGatherer.SessionRunnable>();
	 
	PrintWriter hashWriter;
	static PrintWriter traceWriter;
	private static final boolean LOGGING = false;
	

	InfoHashGatherer info;
	
	NIOConnectionManager connectionManager;
	
	AtomicInteger activeIncomingConnections = new AtomicInteger();
	MetaDataConnectionServer v4srv;
	MetaDataConnectionServer v6srv;
	
	
	static {
		
		try
		{
			if(LOGGING)
				traceWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./trace.log",true))),true);
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static void log(String s)
	{
		if(!LOGGING)
			return;
		traceWriter.println(s);
		traceWriter.flush();
		
	}
	
	
	volatile int numVirtualNodes = 0;
	
	public MetaDataGatherer(InfoHashGatherer info) {
		this.info = info;
		connectionManager = new NIOConnectionManager("mlDHT Indexer NIO Selector ");
		initListeningService();
		
		try
		{
			hashWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./torrents.log",true),"UTF-8")),true);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
		
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try
				{
					if(new File("./torrents/").getUsableSpace() < 512*1024*1024)
						return;
					
					while(dhtLookups() > 0);
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
			}
		}, 20, 1, TimeUnit.SECONDS);
		
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try
				{
					int dhtServers = 0;
					
					for(DHTtype type : DHTtype.values())
					{
						int dhtMax = DHT.getDHT(type).getServerManager().getActiveServerCount();
						if(dhtServers == 0)
							dhtServers = dhtMax;
						else
							dhtServers = Math.min(dhtServers, dhtMax);
					}
					
					numVirtualNodes = dhtServers;
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
			}
		}, 20, 30, TimeUnit.SECONDS);
		
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try
				{
					digestTaskQueue();
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
			}
		}, 20, 3, TimeUnit.SECONDS);
		
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try
				{
					processFinished();
				} catch (Exception e)
				{
					DHT.log(e, LogLevel.Error);
				}
			}
		}, 10*1000, 100, TimeUnit.MILLISECONDS);
	}
	
	
	private void initListeningService() {
		
		IncomingConnectionHandler handler = new IncomingConnectionHandler() {
			public boolean canAccept() {
				return activeIncomingConnections.get() < numVirtualNodes * MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE;
			}
			
			public void acceptedConnection(SocketChannel chan) {
				final PullMetaDataConnection conn = new PullMetaDataConnection(chan);
				conn.metaHandler = new MetaConnectionHandler() {
					public void onTerminate(boolean wasConnected) {
						if(wasConnected)
							activeIncomingConnections.decrementAndGet();
						
						if(conn.isState(PullMetaDataConnection.STATE_METADATA_VERIFIED))
						{
							// offload IO/DB stuff from the networking thread
							DHTIndexer.indexerScheduler.submit(new Runnable() {
								public void run() {
									Session s = HibernateUtil.getSessionFactory().openSession();
									try {
										
										TorrentDBEntry entry = (TorrentDBEntry) s.createCriteria(TorrentDBEntry.class).add(Restrictions.eq("info_hash", conn.infoHash)).uniqueResult();
										FetchTask task = new FetchTask();
										task.entry = entry;
										task.hash = new Key(entry.info_hash).toString(false);

										// and immediately finish it. we don't use the queue to avoid deadlocks
										writeTorrentFile(conn, task);
										fetchTaskTerminated(task, 2);
									} catch (IOException e)
									{
										DHT.log(e, LogLevel.Error);
									} finally {
										s.close();
									}
								}
							});
						}

					}
					
					@Override
					public void onConnect() {
						activeIncomingConnections.incrementAndGet();
					}
				};
				
				conn.checker = new InfohashChecker() {
					public boolean isInfohashAcceptable(byte[] hash) {
						return info.incomingCanidates.contains(new Key(hash));
					}
				};
				
				connectionManager.register(conn);
			}
		};
		
		v4srv = new MetaDataConnectionServer(Inet4Address.class);
		v6srv = new MetaDataConnectionServer(Inet6Address.class);
		v4srv.connectionHandler = handler;
		v6srv.connectionHandler = handler;
		connectionManager.register(v4srv);
		connectionManager.register(v6srv);
	}
	
	private void updatePivots() {
		int numPivots = Math.max(MIN_PIVOTS, numVirtualNodes / PIVOT_EVERY_N_VIRTUAL_NODES);
		Key k = Key.createRandomKey();
		int i = 0;
		while(lookupPivotPoints.size() < numPivots)
			lookupPivotPoints.add(k.getDerivedKey(i++));
		while(lookupPivotPoints.size() > numPivots)
			lookupPivotPoints.remove(lookupPivotPoints.size()-1);
		
		Collections.sort(lookupPivotPoints);
	}
	
	private int dhtLookups() {
		updatePivots();
		Set<TorrentDBEntry> entries = getLookupCandidates();
		
		int started = 0;
		
		for(final TorrentDBEntry entry : entries)
		{
			final FetchTask task = new FetchTask();
			task.entry = entry;
			task.addresses = new ArrayList<PeerAddressDBItem>();
			task.hash = new Key(entry.info_hash).toString(false);;
			
			log("starting DHT lookup for "+task.hash);

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

						log("one DHT lookup done for "+task.hash);
					}

					if(pending == 0)
					{
						log("all DHT lookups done for"+task.hash);
						activeLookups.decrementAndGet();
						scrapeHandler.process();
						
						if(task.entry.status == TorrentDBEntry.STATE_CURRENTLY_ATTEMPTING_TO_FETCH)
						{
							task.addresses.removeAll(Collections.singleton(null));
							if(task.addresses.size() > 0) {
								// trim to a limited amount of addresses to avoid 1 task being stuck for ages
								if(task.addresses.size() > MAX_ATTEMPTS_PER_INFOHASH)
									task.addresses.subList(MAX_ATTEMPTS_PER_INFOHASH, task.addresses.size()).clear();
								fetchTasks.add(task);
								log("added metadata task based on DHT for "+task.hash);
							} else {
								log("found no DHT entires for "+task.hash);
								fetchTaskTerminated(task, 0);
							}
							
						} else {
							fetchTaskTerminated(task, -1);
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
				started++;
			} else // this usually shouldn't happen but sometimes no RPC server may be availble despite numVirtualNodes > 0
				fetchTaskTerminated(task, 0);

		}
		
		return started;
	}

	
	private Set<TorrentDBEntry> getLookupCandidates() {
		Set<TorrentDBEntry> toLookup = new TreeSet<TorrentDBEntry>();
		//TorrentDBEntry result = null;
		
		
		int queuedConnections = fetchTasks.size();
		int activeAndQueuedConnections = activeOutgoingConnections.get() + queuedConnections;
		int maxConnections = numVirtualNodes * MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE;
		int currentLookups = activeLookups.get();
		int maxLookups = Math.min(MAX_LOOKUPS_PER_PIVOT, numVirtualNodes * DHTConstants.MAX_ACTIVE_TASKS - currentLookups);
		
		int maxMetaGetLookups = 0;
		// too few connections, let's do as many lookups as we can
		if(activeAndQueuedConnections <= maxConnections)
			maxMetaGetLookups = maxLookups;
		else if(queuedConnections < maxConnections)
		{ // we have enough connections, but lets do some more lookups to fill the queue
			// as the buffer fills up we scale down the number of lookups we will do
			maxMetaGetLookups = Math.min(maxConnections, maxLookups);
		}
		
		if(maxLookups < 1)
			return Collections.EMPTY_SET;
		
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		ArrayList<Key> pivotBackup = (ArrayList<Key>) lookupPivotPoints.clone();

		try
		{

			int pivotIdx = ThreadLocalUtils.getThreadLocalRandom().nextInt(lookupPivotPoints.size());
			Key startKey = lookupPivotPoints.get(pivotIdx);
			Key endKey = pivotIdx+1 == lookupPivotPoints.size() ? Key.MAX_KEY : lookupPivotPoints.get(pivotIdx+1);

			
			int wants = maxMetaGetLookups;

			List<TorrentDBEntry> results = Collections.EMPTY_LIST;

			// get canidates for combined scrape+retrieve
			if(wants > 0)
			{
				String query = "from ihdata e where useindex(e, infohashIdx) is true and (e.info_hash > ? and e.info_hash < ?) and e.status = ? and e.hitCount > 0 and e.lastLookupTime < ? order by e.info_hash";

				results = session.createQuery(query)
				.setBinary(0, startKey.getHash())
				.setBinary(1, endKey.getHash())
				.setInteger(2, TorrentDBEntry.STATE_METADATA_NEVER_OBTAINED)
				.setLong(3, System.currentTimeMillis()/1000 - 3600) // don't fetch more than once an hour
				.setFirstResult(0)
				//.setComment("useIndex(e,primary)")
				.setMaxResults(wants)
				.setFetchSize(wants)
				.list();


				lookupPivotPoints.remove(startKey);
				if(results.size() > 0)
				{
					System.out.println(startKey+" -> "+new Key(results.get(results.size()-1).info_hash));
					lookupPivotPoints.add(new Key(results.get(results.size()-1).info_hash));
				} else if(endKey == Key.MAX_KEY)
				{
					System.out.println(startKey+" -> "+Key.MIN_KEY);
					lookupPivotPoints.add(Key.MIN_KEY);
				} else
					System.out.println(startKey+" -> -");
					

					
				Collections.sort(lookupPivotPoints);				
				

				toLookup.addAll(results);
			}

			wants = maxLookups - toLookup.size();


			// get canidates for scrape
			if(wants > 0)
			{
				String query = "from ihdata e where useindex(e, infohashIdx) is true and (e.info_hash > ? and e.info_hash < ?) and e.status >= ? and e.hitCount > 0 and e.lastLookupTime < ? order by e.hitCount desc, e.info_hash asc";

				results = session.createQuery(query)
				.setBinary(0, startKey.getHash())
				.setBinary(1, endKey.getHash())
				.setInteger(2, TorrentDBEntry.STATE_METADATA_RETRIEVED_PENDING_UPLOAD)
				.setLong(3, System.currentTimeMillis()/1000 - 3600*12) // don't fetch more than once every 12 hours
				.setFirstResult(0)
				//.setComment("useIndex(e,primary)")
				.setMaxResults(wants)
				.setFetchSize(wants)
				.list();

				toLookup.addAll(results);
			}

			// update in ascending order to avoid deadlocks
			long now = System.currentTimeMillis()/1000;
			for(TorrentDBEntry e : toLookup)
			{
				e.lastLookupTime = now;
				if(e.status == 0)
					e.status = 1;
				session.update(e);
			}
				

			tx.commit();
		} catch(Exception e)
		{
			tx.rollback();
			DHT.log(e, LogLevel.Info);
			// restore on failure
			lookupPivotPoints = pivotBackup;
			// rolled back, don't start lookups
			return Collections.EMPTY_SET;
		} finally {
			session.close();
		}
		
		return toLookup;
	}
	
	
	private void digestTaskQueue() {
		while(activeOutgoingConnections.get() < numVirtualNodes * MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE)
		{
			FetchTask task = fetchTasks.poll();
			if(task == null)
				break;
			fetchMetadata(task);
		}
	}
	
	private void writeTorrentFile(PullMetaDataConnection connection, FetchTask task) throws IOException
	{
		Map<String, Object> infoMap = new BDecoder().decodeByteBuffer(connection.metaData, false);
		Map<String, Object> rootMap = new HashMap<String, Object>();
		rootMap.put("info", infoMap);
		rootMap.put("announce", "dht://"+task.hash);

		byte[] torrent = BEncoder.encode(rootMap);


		byte[] rawName = (byte[])infoMap.get("name.utf-8");
		if(rawName == null)
			rawName = (byte[])infoMap.get("name");
		String name = rawName != null ? new String(rawName,"UTF-8") : "";


		String hash = task.hash;

		hashWriter.println(hash +"\t"+ name);
		hashWriter.flush();

		File f = new File("./torrents/"+hash.substring(0, 2)+"/"+hash.substring(2,4)+"/"+hash+".torrent");
		f.getParentFile().mkdirs();

		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(f, "rw");
			raf.setLength(torrent.length);
			raf.write(torrent);
		} finally {
			raf.close();
		}

		log("successful metadata connection for "+task.hash);							
	}

	private void fetchMetadata(final FetchTask task) {
		PeerAddressDBItem item = task.addresses.get(0);
		task.addresses.remove(item);

		InetSocketAddress addr = new InetSocketAddress(item.getInetAddress(),item.getPort());

		final PullMetaDataConnection conn = new PullMetaDataConnection(task.entry.info_hash, addr);
		if(task.previousConnection != null)
			conn.resume(task.previousConnection);

		conn.metaHandler = new MetaConnectionHandler() {
			public void onTerminate(boolean wasConnected) {
				if(!conn.isState(PullMetaDataConnection.STATE_METADATA_VERIFIED))
				{
					if(task.addresses.size() > 0)
					{
						log("failed metadata connection and requeueing for "+task.hash);
						task.previousConnection = conn;
						fetchMetadata(task);
					} else {
						log("failed metadata connection and finished for "+task.hash);
						fetchTaskTerminated(task, 0);
						digestTaskQueue();
					}
				} else
				{
					try
					{
						writeTorrentFile(conn, task);
						fetchTaskTerminated(task, 2);
					} catch (IOException e)
					{
						e.printStackTrace();
						log("successful metadata connection but failed to store for "+task.hash);
						fetchTaskTerminated(task, 0);
					}
				}

				if(wasConnected)
					activeOutgoingConnections.decrementAndGet();
			}
			
			public void onConnect() {
				activeOutgoingConnections.incrementAndGet();
			}
		};
		// connect after registering the handler in case we get an immediate terminate event
		connectionManager.register(conn);
		log("starting metadata connection for "+task.hash);
	}
	
	
	private void fetchTaskTerminated(final FetchTask t, final int newStatus) {
		
		toFinish.add(new SessionRunnable() {
			
			{ // initializer
				entry = t.entry;
				key = new Key(entry.info_hash);
			}
			
			public void run() {
				try {
					entry = (TorrentDBEntry) session.get(TorrentDBEntry.class, entry.id);
				} catch(ObjectNotFoundException ex)
				{ 	// handle concurrent deletes
					session.save(entry);
				} /* catch(NonUniqueObjectException ex)
				{
					entry = (TorrentDBEntry) session.get(TorrentDBEntry.class, entry.id);
					session.refresh(entry);
				} */
				
				
				long now = System.currentTimeMillis()/1000;
				entry.lastLookupTime = now;
				
				// attempted to fetch, update status accordingly
				if(entry.status == TorrentDBEntry.STATE_CURRENTLY_ATTEMPTING_TO_FETCH)
				{
					entry.status = newStatus;
					entry.fetchAttemptCount++;
					entry.hitCount /= 2;
				}
				
				// looked this up for scrapes, just update the hit count
				if(entry.status >= TorrentDBEntry.STATE_METADATA_RETRIEVED_PENDING_UPLOAD)
				{
					info.incomingCanidates.remove(key);
					entry.hitCount = 0;					
				}
				
				if(newStatus == TorrentDBEntry.STATE_METADATA_NEVER_OBTAINED)
					info.incomingCanidates.add(key);

				session.update(entry);
				
				if(t.scrapes != null && (t.scrapes.getScrapedPeers() > 0 || t.scrapes.getScrapedSeeds() > 0 || t.addresses.size() > 0))
				{
					ScrapeDBEntry scrape = new ScrapeDBEntry();
					scrape.created = now;
					scrape.leechers = t.scrapes.getScrapedPeers();
					scrape.seeds = t.scrapes.getScrapedSeeds();
					scrape.direct = t.scrapes.getDirectResultCount();
					scrape.torrent = entry;
					session.save(scrape);
				}

				log("torrent done for "+key.toString(false)+" | new status "+newStatus);
			}
		});
	}
	
	void processFinished() {
		

		
		ArrayList<SessionRunnable> toProcess = new ArrayList<SessionRunnable>();
		SessionRunnable toDrain = null;
		while((toDrain = toFinish.poll()) != null)
			toProcess.add(toDrain);
		Collections.sort(toProcess);
		
		Set<Key> processedKeys = new HashSet<Key>();
		List<Integer> ids = new ArrayList<Integer>();
		int i = 0;
		
		
		Session s = HibernateUtil.getSessionFactory().openSession();
		try {
			while(i<toProcess.size())
			{
				Transaction tx = s.beginTransaction();
				int startIdx = i;

				try
				{
					
					SessionRunnable toRun = null;
					processedKeys.clear();
					
					while(i-startIdx < MAX_FINISHED_UPDATE_CHARGE && i<toProcess.size())
					{
						toRun = toProcess.get(i);
						
						toRun.session = s;
						if(!processedKeys.contains(toRun.key))
						{ // only process if we haven't already touched the same row during this transaction. avoids a lot of headaches
							processedKeys.add(toRun.key);
							ids.add(toRun.entry.id);
						} else {
							break;
						}
						
						i++;
					}
					
					
					s.createCriteria(TorrentDBEntry.class).add(Restrictions.in("id", ids)).list();
					
					for(int j=startIdx;j<i;j++)
						toProcess.get(j).run();
					
					tx.commit();
					s.clear();
				} catch (HibernateException e)
				{
					tx.rollback();
					// something deadlocked? put everything back into the todo list that hasn't been processed yet
					toFinish.addAll(toProcess.subList(startIdx, toProcess.size()));
					DHT.log(e, LogLevel.Error);
					break;
				}
			}
		} finally {
			s.close();
		}
	}


	class FetchTask {
		TorrentDBEntry entry;
		String hash;
		List<PeerAddressDBItem> addresses;
		ScrapeResponseHandler scrapes;
		PullMetaDataConnection previousConnection;
	}


	private abstract class SessionRunnable implements Runnable, Comparable<SessionRunnable> {
		
		Session session;
		Key key;
		TorrentDBEntry entry;
		
		public int compareTo(SessionRunnable o) {
			return key.compareTo(o.key);
		}
	}
}
