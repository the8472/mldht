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


import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.hibernate.*;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import lbms.plugins.mldht.indexer.utils.ARCwithBlue;
import lbms.plugins.mldht.indexer.utils.Blue;
import lbms.plugins.mldht.indexer.utils.BlueState;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTIndexingListener;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.DHT.LogLevel;

public class InfoHashGatherer implements DHTIndexingListener {

	private static final int MAX_BATCH_SIZE = 300;
	private static final int MAX_BUFFER_SIZE = 1000000;
	private static final int MAX_RECENTLY_SEEN = 1000000;

	boolean running = true;
	ConcurrentLinkedQueue<Key> handoffQueue = new ConcurrentLinkedQueue<Key>();
	
	ARCwithBlue<Key, Key> recentlyProcessed = new ARCwithBlue<Key, Key>(MAX_BATCH_SIZE,MAX_RECENTLY_SEEN);
	boolean keysOverflow;
	boolean keysUnderflow;
	
	NavigableSet<Key> keys = new TreeSet<Key>();
	Key pivot = Key.MIN_KEY;
	
	
	ConcurrentSkipListSet<Key> incomingCanidates = new ConcurrentSkipListSet<Key>();
	Blue incomingLimiter = new Blue(0.001f);
	
	MetaDataGatherer meta;
	
	public void setMetaDataGatherer(MetaDataGatherer meta)
	{
		this.meta = meta;
	}
	
	
	
	
	void processQueue() {
		// draining things from a queue into a set to eleminate redundancies
		synchronized (keys)
		{
			Key toProcess = null;
			while(keys.size() < MAX_BUFFER_SIZE && (toProcess = handoffQueue.poll()) != null)
				if(recentlyProcessed.get(toProcess) == null) // access recently processed entries to get LRU behavior
					keys.add(toProcess);
					
			if(keys.size() >= MAX_BUFFER_SIZE)
			{
				keysOverflow = true;
				handoffQueue.clear();
			}
				
		}
	}
	
	void resizeRecentlySeen() {
		synchronized (keys)
		{
			if(keysOverflow && !keysUnderflow)
				recentlyProcessed.adjust(BlueState.QUEUE_FULL);
			if(keysUnderflow && !keysOverflow)
				recentlyProcessed.adjust(BlueState.QUEUE_EMPTY);
			
			keysUnderflow = false;
			keysOverflow = false;
		}
		
		if(meta == null)
			return;
		
		int inc = meta.activeIncomingConnections.get();
		if(inc <= 0)
			incomingLimiter.update(BlueState.QUEUE_EMPTY);
		int toRemove = 0;
		if(inc >= meta.getNumVirtualNodes() * MetaDataGatherer.MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE)
		{
			incomingLimiter.update(BlueState.QUEUE_FULL);
			toRemove = MAX_BATCH_SIZE;
		}
				
		toRemove = Math.max(toRemove,toRemove+incomingCanidates.size()-MAX_RECENTLY_SEEN);
		for(Iterator it = incomingCanidates.tailSet(Key.createRandomKey()).iterator();it.hasNext() && toRemove > 0;toRemove--)
		{
			it.next();it.remove();
		}
	}
	
	
	void processInfohashes() {
		Session session = HibernateUtil.getSessionFactory().openSession();
		List<Key> keysToDump = new ArrayList<Key>();
		
		synchronized (keys)
		{
			if(keys.isEmpty())
				keysUnderflow = true;			
		}
		
		while(true)
		{
			synchronized (keys)
			{
				if(keys.isEmpty())
					break;
				
				Set<Key> tailSet = keys.tailSet(pivot);
				if(tailSet.isEmpty())
				{
					pivot = Key.MIN_KEY;
					continue;
				}
				
				for(Iterator<Key> it = tailSet.iterator();it.hasNext() && keysToDump.size() < MAX_BATCH_SIZE;)
				{
					keysToDump.add(it.next());
					it.remove();
				}
				
				pivot = keysToDump.get(keysToDump.size()-1);
			
				// even if the lookup fails they'll just get requeued, no need to process the incoming queries for these hashes again
				for(Key k : keysToDump)
					recentlyProcessed.put(k, k);
			}
			
			if(keysToDump.isEmpty())
				break;
			
			
			Transaction tx = session.beginTransaction();			
			
			try
			{
				Map<Key, byte[]> hashesRaw = new HashMap<Key, byte[]>();
				for(Key k : keysToDump)
					hashesRaw.put(k,k.getHash());

				
				List<byte[]> existingHashesRaw = session.createCriteria(TorrentDBEntry.class)
					.add(Restrictions.in("info_hash",hashesRaw.values()))
					.setProjection(Projections.property("info_hash"))
					.setFetchSize(hashesRaw.size())
					.list();
				
				List<Key> existingHashes = new ArrayList<Key>();
				for(byte[] b : existingHashesRaw)
					existingHashes.add(new Key(b));
				
				long nowStamp = System.currentTimeMillis()/1000;
				
				for(Key k: keysToDump)
				{
					if(existingHashes.contains(k))
						continue;
					TorrentDBEntry entry = new TorrentDBEntry();
					entry.info_hash = hashesRaw.get(k);
					entry.added = nowStamp;
					entry.lastSeen = nowStamp;
					entry.hitCount = 0;
					//System.out.println("adding entry for "+entry.info_hash+" updated:"+entry.added);
					entry.status = 0;
					session.save(entry);
					
					incomingCanidates.add(k);						
				}
				
				session.flush();
				
				session.createQuery("update ihdata e set e.hitCount = e.hitCount+1, e.lastSeen = (e.lastSeen * 0.97 + :tstamp) where e.info_hash in (:hashes)")
					.setParameter("tstamp", nowStamp * 0.03)
					.setParameterList("hashes", hashesRaw.values())
					.executeUpdate();

				tx.commit();
			} catch (Exception e)
			{
				synchronized (keys)
				{
					keys.addAll(keysToDump);
				}
				tx.rollback();
				e.printStackTrace();
				// avoid deadlocking again and again
				break;
			} finally {
				session.clear();
				keysToDump.clear();
			}
		}
		
		session.close();
			
		
	}
	
	//PrintWriter hashWriter;
	
	public InfoHashGatherer() {
		/*
		try
		{
			hashWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./infohashQueries.log",true))),false);
		} catch (Exception e)
		{
			// TODO: handle exception
		}*/
		
		Runnable hashesToDB = new Runnable() {
			public void run() {
				processInfohashes(); 
			}
		};
		
		Runnable adaptSize = new Runnable() {
			public void run() {
				try {
					resizeRecentlySeen();
				} catch (Exception e) {
					DHT.log(e, LogLevel.Error);
				}
			}
		};
		
		// only run this task every 10 seconds. but then it'll loop to batch-insert everything down to the low water mark
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(hashesToDB, 1000, 10, TimeUnit.SECONDS);
		

		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(adaptSize, 1, 1, TimeUnit.SECONDS);
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				processQueue(); 
			}
		}, 1000, 500, TimeUnit.MILLISECONDS);

		
	}
	
	public List<PeerAddressDBItem> incomingPeersRequest(Key infoHash, InetAddress sourceAddress, Key nodeID) {
		/*hashWriter.println(System.currentTimeMillis()+"\t"+infoHash.toString(false)+"\tfrom: "+sourceAddress.getHostAddress()+"/"+nodeID.toString(false));
		if(DHT.getThreadLocalRandom().nextInt(100) == 0)
			hashWriter.flush();*/
		
		handoffQueue.add(infoHash);
		if(meta != null)
		{
			if(incomingCanidates.contains(infoHash) && !incomingLimiter.doDrop())
			{
				if(sourceAddress instanceof Inet6Address)
					return Collections.singletonList(PeerAddressDBItem.createFromAddress(meta.v6srv.addr, meta.v6srv.port, false));
				return Collections.singletonList(PeerAddressDBItem.createFromAddress(meta.v4srv.addr, meta.v4srv.port, false));
			}
		}
		return Collections.EMPTY_LIST;
	}

	
}
