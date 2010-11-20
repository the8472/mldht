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
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.gudy.azureus2.core3.util.BDecoder;
import org.gudy.azureus2.core3.util.BEncoder;
import org.hibernate.*;
import org.hibernate.criterion.*;


import lbms.plugins.mldht.indexer.MetaDataConnectionServer.IncomingConnectionHandler;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.InfohashChecker;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.MetaConnectionHandler;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.tasks.TaskListener;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

public class MetaDataGatherer {
	
	public static final int MAX_PEERS_PER_INFOHASH = 40;
	public static final int LOOKUPS_PER_VIRTUAL_NODE = 3;
	public static final int PIVOT_EVERY_N_VIRTUAL_NODES = 3;
	public static final int MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE = 2;
	public static final int MAX_FINISHED_UPDATE_CHARGE = 100;

	ArrayList<Key> lookupPivotPoints = new ArrayList<Key>();
	AtomicInteger activeLookups = new AtomicInteger();
	AtomicInteger activeOutgoingConnections = new AtomicInteger();
	ConcurrentLinkedQueue<FetchTask> fetchTasks = new ConcurrentLinkedQueue<FetchTask>();
	
	ConcurrentLinkedQueue<SessionRunnable> toFinish = new ConcurrentLinkedQueue<MetaDataGatherer.SessionRunnable>();
	 
	PrintWriter hashWriter;
	static PrintWriter traceWriter;
	private static final boolean LOGGING = false;
	

	InfoHashGatherer info;
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
					dhtLookup();
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
						int dhtMax = DHT.getDHT(type).getServers().size();
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
										fetchTaskTerminated(entry, 2);
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
				
				conn.register();
			}
		};
		
		v4srv = new MetaDataConnectionServer(Inet4Address.class);
		v6srv = new MetaDataConnectionServer(Inet6Address.class);
		v4srv.connectionHandler = handler;
		v6srv.connectionHandler = handler;
		v4srv.register();
		v6srv.register();
		
		
	}
	
	private void updatePivots() {
		int numPivots = Math.max(2, numVirtualNodes / PIVOT_EVERY_N_VIRTUAL_NODES);
		Key k = Key.createRandomKey();
		int i = 0;
		while(lookupPivotPoints.size() < numPivots)
			lookupPivotPoints.add(k.getDerivedKey(i++));
		while(lookupPivotPoints.size() > numPivots)
			lookupPivotPoints.remove(lookupPivotPoints.size()-1);
	}
	
	
	
	private void dhtLookup() {
		Set<TorrentDBEntry> toLookup = new TreeSet<TorrentDBEntry>();
		//TorrentDBEntry result = null;
		
		updatePivots();
		
		int currentPool = activeOutgoingConnections.get() + fetchTasks.size();
		int targetPool = numVirtualNodes * MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE;
		int currentLookups = activeLookups.get();
		int maxLookups = numVirtualNodes * LOOKUPS_PER_VIRTUAL_NODE;
		
		int targetNum = 0;
		if(currentPool <= targetPool)
			targetNum = maxLookups - currentLookups;
		if(currentPool > targetPool && currentPool < 2 * targetPool)
		{ // exceeding target, leave some breathing room to fill up the buffer up to 2 * target
			currentPool = currentPool - targetPool;
			
			// scale max down by the percentage left until the buffer is full too
			maxLookups = (int) (maxLookups * (1.0- currentPool*1.0/targetPool));
			targetNum = maxLookups - currentLookups;
		}
			
		
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		ArrayList<Key> pivotBackup = (ArrayList<Key>) lookupPivotPoints.clone();

		try
		{
			while(true)
			{

				Collections.sort(lookupPivotPoints);

				Key startKey = Key.MIN_KEY;
				Key ceilKey = Key.MAX_KEY;
				
				int pivotIdx = ThreadLocalUtils.getThreadLocalRandom().nextInt(lookupPivotPoints.size());
				startKey = lookupPivotPoints.get(pivotIdx);
				ceilKey = startKey;
				// skip identical keys to find the next-higher one
				while(++pivotIdx  < lookupPivotPoints.size() && (ceilKey = lookupPivotPoints.get(pivotIdx)).equals(startKey))
					;
				if(ceilKey.equals(startKey))
					ceilKey = Key.MAX_KEY;

				// due to the prefix-increment we'll already be +1ed here
				boolean possibleWraparound = pivotIdx >= lookupPivotPoints.size();

				int wants = Math.min(targetNum, LOOKUPS_PER_VIRTUAL_NODE * PIVOT_EVERY_N_VIRTUAL_NODES);
				if(wants < 1)
					break;

				List<TorrentDBEntry> results = session.createQuery("from ihdata e where useindex(e, infohashIdx) is true and e.info_hash > ? and e.info_hash < ? and e.status = 0 and e.hitCount > 0 and e.lastFetchAttempt < ? order by e.info_hash")
					.setBinary(0, startKey.getHash())
					.setBinary(1, ceilKey.getHash())
					.setLong(2, System.currentTimeMillis()/1000 - 3600)
					.setFirstResult(0)
					//.setComment("useIndex(e,primary)")
					.setMaxResults(wants)
					.setFetchSize(wants)
					.list();


				//from ihdata where status = 0 and hitcount = (SELECT MAX(hitcount) FROM ihdata where status = 0 and fetchAttemptCount = (SELECT MIN(fetchAttemptCount) FROM ihdata WHERE status = 0)) and fetchAttemptCount = (SELECT MIN(fetchAttemptCount) FROM ihdata WHERE status = 0) ORDER BY info_hash asc limit 1


				if(!results.isEmpty())
				{
					lookupPivotPoints.remove(startKey);
					lookupPivotPoints.add(new Key(results.get(results.size()-1).info_hash));
					//System.out.println("lookup for "+entry.info_hash+" updated:"+entry.added);
					
					targetNum-=results.size();
					toLookup.addAll(results);
				} else if(possibleWraparound) {
					// wrap-around, start from the beginning
					lookupPivotPoints.remove(startKey);
					lookupPivotPoints.add(Key.MIN_KEY); 
				} else if(startKey.equals(Key.MIN_KEY) && ceilKey.equals(Key.MAX_KEY))
				{ // scanned everything and found nothing, avoid infinite loops
					break;
				} else
				{
					// we found nothing based on this pivot point. assume we caught up to the previous key... next round will generate a new one.
					lookupPivotPoints.remove(startKey);
				}


			}

			// update in order to avoid deadlocks
			long now = System.currentTimeMillis()/1000;
			for(TorrentDBEntry e : toLookup)
			{
				e.lastFetchAttempt = now;
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
			return;
		} finally {
			session.close();
		}
		
		for(final TorrentDBEntry entry : toLookup)
		{
			final FetchTask task = new FetchTask();

			task.entry = entry;
			task.addresses = new ArrayList<PeerAddressDBItem>();

			

			task.hash = new Key(entry.info_hash).toString(false);;
			
			log("starting DHT lookup for "+task.hash);

			final AtomicInteger pendingLookups = new AtomicInteger();

			TaskListener lookupListener = new TaskListener() {
				public void finished(Task t) {
					int pending = pendingLookups.decrementAndGet();
					if(pending >= 0)
					{
						PeerLookupTask pt = (PeerLookupTask) t;
						task.addresses.addAll(pt.getReturnedItems());
						log("one DHT lookup done for "+task.hash);
					}

					if(pending == 0)
					{
						log("all DHT lookups done for"+task.hash);
						activeLookups.decrementAndGet();
						// remove all null entries that might have occured due to concurrent inserts
						task.addresses.removeAll(Collections.singleton(null));
						if(task.addresses.size() > 0) {
							// trim to a limited amount of addresses to avoid 1 task being stuck for ages
							if(task.addresses.size() > MAX_PEERS_PER_INFOHASH)
								task.addresses.subList(MAX_PEERS_PER_INFOHASH, task.addresses.size()).clear();
							fetchTasks.add(task);
							log("added metadata task based on DHT for "+task.hash);
						} else {
							log("found no DHT entires for "+task.hash);
							fetchTaskTerminated(task.entry, 0);
						}
					}
				}
			};

			activeLookups.incrementAndGet();

			for(DHTtype type : DHTtype.values())
			{
				DHT dht = DHT.getDHT(type);
				PeerLookupTask lookupTask = dht.createPeerLookup(task.entry.info_hash);
				pendingLookups.incrementAndGet();
				lookupTask.setFastLookup(true);
				lookupTask.setScrapeOnly(true);
				lookupTask.addListener(lookupListener);
				lookupTask.setInfo("Grabbing .torrent for "+task.hash);
				lookupTask.setNoSeeds(false);
				dht.getTaskManager().addTask(lookupTask);
			}

		}
		
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

		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		raf.write(torrent);
		raf.close();

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
						fetchTaskTerminated(task.entry, 0);
						digestTaskQueue();
					}
				} else
				{
					try
					{
						writeTorrentFile(conn, task);
						fetchTaskTerminated(task.entry, 2);
					} catch (IOException e)
					{
						e.printStackTrace();
						log("successful metadata connection but failed to store for "+task.hash);
						fetchTaskTerminated(task.entry, 0);
					}
				}

				if(wasConnected)
					activeOutgoingConnections.decrementAndGet();
			}
			
			@Override
			public void onConnect() {
				activeOutgoingConnections.incrementAndGet();
			}
		};
		// connect after registering the handler in case we get an immediate terminate event
		conn.register();
		log("starting metadata connection for "+task.hash);
	}
	
	
	private void fetchTaskTerminated(final TorrentDBEntry e, final int newStatus) {
		
		toFinish.add(new SessionRunnable() {
			
			{ // initializer
				key = new Key(e.info_hash);
				entry = e;
			}
			
			public int compareTo(SessionRunnable o) {
				return key.compareTo(o.key);
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
				
				
				
				// we already finished this one and another lookup was done in parallel. ignore.
				if(entry.status > 1)
				{
					info.incomingCanidates.remove(key);
					return;
				}
					
				
				

				entry.status = newStatus;
				long now = System.currentTimeMillis()/1000;
				entry.lastFetchAttempt = now;
				entry.fetchAttemptCount++;


				if(newStatus == 0)
				{
					entry.hitCount /= 2;
					info.incomingCanidates.add(key);						
				} else
				{
					info.incomingCanidates.remove(key);						
				}
					

				/*
					// remove entries that failed too often (hit count gets reduced on failure)
					if(entry.hitCount <= 0)
						session.delete(entry);
					else
						session.update(entry);
				 */
				session.update(entry);


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
		PullMetaDataConnection previousConnection;
	}


	private abstract class SessionRunnable implements Runnable, Comparable<SessionRunnable> {
		
		Session session;
		Key key;
		TorrentDBEntry entry;
	}
	
	
	
}
