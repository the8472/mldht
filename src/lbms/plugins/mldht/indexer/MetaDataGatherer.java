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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.gudy.azureus2.core3.util.BDecoder;
import org.gudy.azureus2.core3.util.BEncoder;
import org.hibernate.*;
import org.hibernate.criterion.*;



import lbms.plugins.mldht.indexer.MetaDataConnectionServer.IncomingConnectionHandler;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.InfohashChecker;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.MetaConnectionHandler;
import lbms.plugins.mldht.indexer.assemblyline.AssemblyRunner;
import lbms.plugins.mldht.indexer.assemblyline.SoftCapacityQueue;
import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.tasks.TaskListener;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import lbms.plugins.mldht.utils.NIOConnectionManager;

public class MetaDataGatherer {
	
	public static final int MAX_ATTEMPTS_PER_INFOHASH = 50;
	public static final int LOOKUPS_PER_VIRTUAL_NODE = 3;
	public static final int MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE = 3;
	

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
	
	AtomicInteger activeOutgoingConnections = new AtomicInteger();
	LinkedBlockingQueue<TorrentDBEntry> fetchDHTlink = new SoftCapacityQueue<TorrentDBEntry>(500);
	LinkedBlockingQueue<TorrentDBEntry> scrapeDHTlink = new SoftCapacityQueue<TorrentDBEntry>(500);
	LinkedBlockingQueue<FetchTask> toFetchLink = new SoftCapacityQueue<MetaDataGatherer.FetchTask>(100);
	LinkedBlockingQueue<BatchQuery> terminatedTasks = new SoftCapacityQueue<BatchQuery>(1000);
	
	public MetaDataGatherer(InfoHashGatherer info) {
		this.info = info;
		connectionManager = new NIOConnectionManager("mlDHT Indexer NIO Selector ");
		
		initListeningService();
		
		
		ScheduledExecutorService pool = DHTIndexer.indexerScheduler;
		
		new AssemblyRunner(new FetchCandidateGenerator(this)).submitToPool(pool, 2000);
		new AssemblyRunner(new ScrapeCandidateGenerator(scrapeDHTlink)).submitToPool(pool, 2000);
		new AssemblyRunner(new CandidateLookups(this,fetchDHTlink, scrapeDHTlink,toFetchLink,terminatedTasks)).submitToPool(pool, 100);
		new AssemblyRunner(new TorrentFetcher(this)).submitToPool(pool, 1000);
		new AssemblyRunner(new OrderedBatchQueryRunner(terminatedTasks)).submitToPool(pool, 10000);
		
		
		try
		{
			hashWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./torrents.log",true),"UTF-8")),true);
		} catch (Exception e)
		{
			e.printStackTrace();
		}

	}
	
	
	int getNumVirtualNodes() {
		int dhtServers = 0;
		
		for(DHTtype type : DHTtype.values())
		{
			int dhtMax = DHT.getDHT(type).getServerManager().getActiveServerCount();
			if(dhtMax == 0)
				continue;
			if(dhtServers == 0)
				dhtServers = dhtMax;
			else
				dhtServers = Math.min(dhtServers, dhtMax);
		}
		
		return dhtServers;
		
	}
	
	private void initListeningService() {
		
		IncomingConnectionHandler handler = new IncomingConnectionHandler() {
			public boolean canAccept() {
				return activeIncomingConnections.get() < getNumVirtualNodes() * MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE;
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
										task.hash = new Key(entry.info_hash);

										// and immediately finish it. we don't use the queue to avoid deadlocks
										writeTorrentFile(conn, task);
										terminatedTasks.add(new SuccessfulTask(task));
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
	

	void writeTorrentFile(PullMetaDataConnection connection, FetchTask task) throws IOException
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


		String hash = task.hash.toString(false);

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
		
		// clean things for the GC
		task.previousConnection = null;

		log("successful metadata connection for "+task.hash);							
	}

	
	BatchQuery createSuccessfulTask(FetchTask t) {
		return new SuccessfulTask(t);
	}
	
	BatchQuery createFailedTask(Key k) {
		return new FailedTask(k);
	}
	
	void saveScrapes(Session session, ScrapeResponseHandler scrapes,  TorrentDBEntry entry)
	{
		if(scrapes != null && (scrapes.getScrapedPeers() > 0 || scrapes.getScrapedSeeds() > 0 || scrapes.getDirectResultCount() > 0))
		{
			ScrapeDBEntry scrape = new ScrapeDBEntry();
			scrape.created = System.currentTimeMillis() / 1000;
			scrape.leechers = scrapes.getScrapedPeers();
			scrape.seeds = scrapes.getScrapedSeeds();
			scrape.direct = scrapes.getDirectResultCount();
			scrape.torrent = entry;
			session.save(scrape);
		}
	}

	private class SuccessfulTask extends BatchQuery {
		FetchTask t;
		
		public SuccessfulTask(FetchTask t) {
			super(t.hash);
			this.t = t;
		}
		
		public void run() {
			session.createQuery("update ihdata e set e.status = 2, e.hitCount = floor(e.hitCount/2), e.lastLookupTime = :time where e.info_hash  = :hash and e.status <= 1")
			.setParameter("time",System.currentTimeMillis()/1000)
			.setParameter("hash", key.getHash())
			.executeUpdate();
			info.incomingCanidates.remove(key);
			saveScrapes(session, t.scrapes, t.entry);
		}

	}
	
	private class FailedTask extends BatchQuery {
		public FailedTask(Key k) {
			super(k);
		}
		
		public void run() {
			session.createQuery("update ihdata e set e.status = 0, e.hitCount = floor(e.hitCount/2), e.lastLookupTime = :time where e.info_hash  = :hash and e.status <= 1")
			.setParameter("time",System.currentTimeMillis()/1000)
			.setParameter("hash", key.getHash())
			.executeUpdate();
			info.incomingCanidates.add(key);
		}
	}
	
	static class FetchTask {
		TorrentDBEntry entry;
		Key hash;
		List<PeerAddressDBItem> addresses;
		ScrapeResponseHandler scrapes;
		PullMetaDataConnection previousConnection;
	}

}
