package lbms.plugins.mldht.kad;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

import lbms.plugins.mldht.azureus.MlDHTPlugin;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.tasks.NodeLookup;
import lbms.plugins.mldht.kad.tasks.PingRefreshTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.tasks.TaskListener;

import org.gudy.azureus2.plugins.utils.Utilities;

/**
 * @author Damokles
 *
 */
public class Node {

	private KBucket[] bucket = new KBucket[160];
	private RPCServerBase srv;
	private int num_receives;
	
	private int numReceivesAtLastCheck;
	private long timeOfLastPingCheck;
	private long timeOfLastReceiveCountChange;
	private long timeOfRecovery;
	private boolean survivalMode;
	private int num_entries;
	
	private static Map<String,Serializable> dataStore;

	/**
	 * @param srv
	 */
	public Node(RPCServerBase srv) {
		this.srv = srv;
		num_receives = 0;
		num_entries = 0;
	}

	/**
	 * An RPC message was received, the node must now update
	 * the right bucket.
	 * @param dh_table The DHT
	 * @param msg The message
	 */
	void recieved (DHTBase dh_table, MessageBase msg) {
		
		KBucketEntry newEntry = new KBucketEntry(msg.getOrigin(), msg.getID());
		newEntry.setVersion(msg.getVersion());

		boolean nodeIDchange = false;
		for (int i = 1; i < 160; i++) {
			if (bucket[i] != null) {
				nodeIDchange |= bucket[i].checkForIDChangeAndNotifyOfResponse(msg);
			}
		}
		
		if(!nodeIDchange)
			insertEntry(newEntry);

		num_receives++;

		int newNumEntries = 0;
		for (int i = 1; i < 160; i++) {
			if (bucket[i] != null) {
				newNumEntries += bucket[i].getNumEntries();
			}
		}
		
		num_entries = newNumEntries;
	}

	public void insertEntry (KBucketEntry entry) {
		int bucketID = getOurID().findApproxKeyDistance(entry.getID());
		// return if bit_on is not good
		if (bucketID >= 160 || bucketID == 0) {
			return;
		}

		if (bucket[bucketID] == null) {
			bucket[bucketID] = new KBucket(srv, this);
		}
		bucket[bucketID].insert(entry);
	}

	/**
	 * @return OurID
	 */
	public Key getOurID () {
		if(dataStore != null)
			return (Key)dataStore.get("commonKey");
		// return a fake key if we're not initialized yet
		return new Key(new byte[20]);
	}

	/**
	 * Find the K closest entries to a key and store them in the KClosestNodesSearch
	 * object.
	 * @param kns The object to store the search results
	 */
	public void findKClosestNodes (KClosestNodesSearch kns) {
		// go over all buckets until
		int target = getOurID().findApproxKeyDistance(kns.getSearchTarget());
		boolean high = true;
		boolean low = true;

		if (bucket[target] != null) {
			bucket[target].findKClosestNodes(kns);
		}

		for (int i = 1; i < bucket.length && (low || high); i++) {
			if (low) {
				if (target - i > 0) {
					if (bucket[target - i] != null) {
						low = bucket[target - i].findKClosestNodes(kns);
					}
				} else {
					low = false;
				}
			}
			if (high) {
				if (target + i < bucket.length) {
					if (bucket[target + i] != null) {
						high = bucket[target + i].findKClosestNodes(kns);
					}
				} else {
					high = false;
				}
			}
		}
	}

	/**
	 * Increase the failed queries count of the bucket entry we sent the message to
	*/
	void onTimeout (MessageBase msg) {
		// don't timeout anything if we don't have a connection
		if(survivalMode)
			return;
		for (int i = 1; i < bucket.length; i++) {
			if (bucket[i] != null && bucket[i].onTimeout(msg.getDestination())) {
				return;
			}
		}
	}

	/**
	 * Check if a buckets needs to be refreshed, and refresh if necesarry.
	 */
	public void doBucketChecks (long now) {
		
		// don't do pings too often if we're not receiving anything (connection might be dead)
		if(num_receives != numReceivesAtLastCheck)
		{
			if(survivalMode)
			{
				if(timeOfRecovery == 0)
				{
					// received a packet! ping entries but don't exist survival mode yet
					timeOfRecovery = now;
					timeOfLastPingCheck = 0;
				}
					
				if(now - timeOfRecovery > DHTConstants.REACHABILITY_RECOVERY)
				{
					// ok, enough time passed, we should have recovered live nodes by now, exit survival mode
					survivalMode = false;
					timeOfRecovery = 0;
				}				
			}

			timeOfLastReceiveCountChange = now;
			numReceivesAtLastCheck = num_receives;
			
		} else if(now - timeOfLastReceiveCountChange > DHTConstants.REACHABILITY_TIMEOUT)
		{
			// haven't seen a packet for too long
			// perform heroics to maintain the routing table from now on
			survivalMode = true;
			srv.getTimeoutFilter().reset();
			timeOfRecovery = 0;
		}
		
		// don't spam the checks if we're not receiving anything.
		// we don't want to cause stray packets somewhere in a network
		if(survivalMode && now - timeOfLastPingCheck > DHTConstants.BOOTSTRAP_MIN_INTERVAL)
			return;
		timeOfLastPingCheck = now;
		
		for (int i = 1; i < bucket.length; i++) {
			KBucket b = bucket[i];
			if(b != null)
			{
				List<KBucketEntry> entries = b.getEntries();
				
				// remove boostrap nodes from our buckets
				if (b.getNumEntries() >= DHTConstants.MAX_ENTRIES_PER_BUCKET)
					for (KBucketEntry entry : entries)
						if (DHTConstants.BOOTSTRAP_NODE_ADDRESSES.contains(entry.getAddress()))
							b.removeEntry(entry, true);
				
				if (b.needsToBeRefreshed())
				{
					// clean out buckets full of bad nodes
					if(!survivalMode)
					{
						boolean allBad = true;
						for(KBucketEntry entry : entries)
							allBad &= entry.isBad();
						
						if(allBad)
							bucket[i] = null;
					}
					
					// if the bucket survived that test, ping it
					if(bucket[i] != null)
					{
						DHT.logDebug("Refreshing Bucket: " + i);
						// the key needs to be the refreshed
						PingRefreshTask nl = srv.getDHT().refreshBucket(b);
						if (nl != null)
						{
							b.setRefreshTask(nl);
							nl.setInfo("Refreshing Bucket #" + i);
						}
					}
				} else if(!survivalMode)
				{
					// only replace 1 bad entry with a replacement bucket entry at a time (per bucket)
					bucket[i].checkBadEntries();
				}					
				
			}
		}

	}

	/**
	 * Check if a buckets needs to be refreshed, and refresh if necesarry
	 *
	 * @param dh_table
	 */
	public void fillBuckets (DHTBase dh_table) {
		boolean foundFirst = false;
		for (int i = 1; i < bucket.length; i++) {
			KBucket b = bucket[i];
			if (b == null && foundFirst) {
				b = bucket[i] = new KBucket(srv, this);
			}

			if (b != null && b.getNumEntries() > 0) {
				foundFirst = true;
			}
			if(!foundFirst) {
				bucket[i] = null;
			}
		}

		for (int i = bucket.length - 1; i >= 1; i--) {
			KBucket b = bucket[i];
			if (b == null) {
				break;
			}
			if (b.getNumEntries() < DHTConstants.MAX_ENTRIES_PER_BUCKET) {
				DHT.logDebug("Filling Bucket: " + i);
				// the key needs to be the refreshed
				NodeLookup nl = dh_table.fillBucket(getOurID().createKeyWithDistance(i), b);
				if (nl != null) {
					b.setRefreshTask(nl);
					nl.setInfo("Filling Bucket #" + i);
				}
			}
		}
	}

	/**
	 * Saves the routing table to a file
	 *
	 * @param file to save to
	 * @throws IOException
	 */
	void saveTable (File file) throws IOException {
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(file));
			HashMap<String,Serializable> tableMap = new HashMap<String, Serializable>();
			
			dataStore.put("table"+srv.getDHT().getType().name(), tableMap);
			
			tableMap.put("oldKey", getOurID());
			tableMap.put("bucket", bucket);
			tableMap.put("log2estimate", srv.getDHT().getEstimator().getRawDistanceEstimate());
			tableMap.put("timestamp", System.currentTimeMillis());
			
			oos.writeObject(dataStore);
			oos.flush();

		} finally {
			if (oos != null) {
				oos.close();
			}
		}
	}
	
	synchronized static void initDataStore(File file)
	{
		if(dataStore != null)
			return;
		
		ObjectInputStream ois = null;
		try {
			if (!file.exists()) {
				return;
			}

			ois = new ObjectInputStream(new FileInputStream(file));
			dataStore = (Map<String, Serializable>) ois.readObject();
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally {
			if(ois != null)
				try { ois.close(); } catch (IOException e) { e.printStackTrace(); }
			
			if(dataStore == null)
			{
				dataStore = new HashMap<String, Serializable>();
				dataStore.put("commonKey", Key.createRandomKey());
			}
		}
		
		if(! MlDHTPlugin.getSingleton().getPluginInterface().getPluginconfig().getPluginBooleanParameter("alwaysRestoreID"))
		{
			dataStore.put("commonKey", Key.createRandomKey());
		}
		
	}

	/**
	 * Loads the routing table from a file
	 *
	 * @param file
	 * @param runWhenLoaded is executed when all load operations are finished
	 * @throws IOException
	 */
	void loadTable (File file, DHT dht, final Runnable runWhenLoaded) {
		boolean runDeferred = false;
		initDataStore(file);

		try {
			Map<String,Serializable> table = (Map<String,Serializable>)dataStore.get("table"+dht.getType().name());
			if(table == null)
				return;

			KBucket[] loadedBuckets = (KBucket[])table.get("bucket");
			Key oldID = (Key)table.get("oldKey");
			dht.getEstimator().setInitialRawDistanceEstimate((Double)table.get("log2estimate"));
			long timestamp = (Long)table.get("timestamp");



			// integrate loaded objects

			int entriesLoaded = 0;

			if (getOurID().equals(oldID)) {
				for (int i = 1; i < loadedBuckets.length; i++) {
					KBucket b = loadedBuckets[i];
					if (b == null) {
						continue;
					}
					if (b.getNumEntries() == 0 && entriesLoaded == 0) {
						b = loadedBuckets[i] = null;
						continue;
					}
					b.setServer(srv);
					b.setNode(this);
					entriesLoaded += b.getNumEntries();
				}
				bucket = loadedBuckets;

			} else {
				bucket = new KBucket[160];

				// we want to insert the oldest entries first, this way we'll retain longest-lived entries over restarts
				SortedSet<KBucketEntry> entries = new TreeSet<KBucketEntry>(
						new Comparator<KBucketEntry>() {
							public int compare (KBucketEntry o1, KBucketEntry o2) {
								return (int) (o1.getCreationTime() - o2
										.getCreationTime());
							}
						});

				for (int i = 0; i < loadedBuckets.length; i++) {
					if (loadedBuckets[i] != null) {
						entries.addAll(loadedBuckets[i].getEntries());
					}
				}

				//since our_id might have changed, the entries have to be reinserted
				for (KBucketEntry entry : entries) {
					int bucketID = getOurID().findApproxKeyDistance(entry.getID());
					// return if bit_on is not good
					if (bucketID >= 160) {
						return;
					}
					/*
					 * insert entries immediately. we can safely add stale entries as
					 * they'll be timed out immediately by a ping lookup and then
					 * replaced from the replacement bucket
					 */
					if (!entry.isBad()) {
						// make the bucket if it doesn't exist
						if (bucket[bucketID] == null) {
							bucket[bucketID] = new KBucket(srv, this);
						}
						bucket[bucketID].insert(entry);
						entriesLoaded++;
					}
				}

			}

			if (entriesLoaded > 0) {
				runDeferred = true;
				PingRefreshTask prt = dht.refreshBuckets(bucket, true);
				prt.setInfo("Pinging cached entries.");
				TaskListener bootstrapListener = new TaskListener() {
					public void finished (Task t) {
						if (runWhenLoaded != null) {
							runWhenLoaded.run();
						}
					}
				};
				prt.addListener(bootstrapListener);
			}

			DHT.logInfo("Loaded " + entriesLoaded + " from cache. Cache was "
					+ ((System.currentTimeMillis() - timestamp) / (60 * 1000))
					+ "min old. Reusing old id = " + oldID.equals(getOurID()));

			return;
		} finally {
			if (!runDeferred && runWhenLoaded != null) {
				runWhenLoaded.run();
			}
		}
	}

	/**
	 * Get the number of entries in the routing table
	 *
	 * @return
	 */
	public int getNumEntriesInRoutingTable () {
		return num_entries;
	}

	public KBucket[] getBuckets () {
		return bucket;
	}

}
