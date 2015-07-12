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
package lbms.plugins.mldht.kad;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.tasks.NodeLookup;
import lbms.plugins.mldht.kad.tasks.PingRefreshTask;
import lbms.plugins.mldht.kad.tasks.Task;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import the8472.utils.CowSet;
import the8472.utils.Pair;
import the8472.utils.io.NetMask;


/**
 * @author Damokles
 *
 */
public class Node {
	
	public static final class RoutingTableEntry implements Comparable<RoutingTableEntry> {
		
		public RoutingTableEntry(Prefix prefix, KBucket bucket) {
			this.prefix = prefix;
			this.bucket = bucket;
		}
		
		public final Prefix prefix;
		private KBucket bucket;
		
		public KBucket getBucket() {
			return bucket;
		}
		
		public int compareTo(RoutingTableEntry o) {
			return prefix.compareTo(o.prefix);
		}
	}

	private Object CoWLock = new Object();
	private volatile List<RoutingTableEntry> routingTableCOW = new ArrayList<RoutingTableEntry>();
	private DHT dht;
	private int num_receives;
	
	private int numReceivesAtLastCheck;
	private long timeOfLastPingCheck;
	private long timeOfLastReceiveCountChange;
	private long timeOfRecovery;
	private int num_entries;
	private final CowSet<Key> usedIDs = new CowSet<>();
	private volatile Map<InetAddress,RoutingTableEntry> knownNodes = new HashMap<InetAddress, RoutingTableEntry>();
	private Map<KBucket, Task> maintenanceTasks = new IdentityHashMap<>();
	
	Collection<NetMask> trustedNodes = Collections.emptyList();
	
	private static Map<String,Serializable> dataStore;

	/**
	 * @param srv
	 */
	public Node(DHT dht) {
		this.dht = dht;
		num_receives = 0;
		num_entries = 0;
		
		routingTableCOW.add(new RoutingTableEntry(new Prefix(), new KBucket(this)));
	}

	/**
	 * An RPC message was received, the node must now update the right bucket.
	 * @param msg The message
	 */
	void recieved(MessageBase msg) {
		InetAddress ip = msg.getOrigin().getAddress();
		Key id = msg.getID();
		
		Optional<RPCCall> associatedCall = Optional.ofNullable(msg.getAssociatedCall());
		Optional<Key> expectedId = associatedCall.map(RPCCall::getExpectedID);
		Optional<Pair<KBucket, KBucketEntry>> entryByIp = bucketForIP(ip);
		
		if(entryByIp.isPresent()) {
			KBucket bucket = entryByIp.get().a;
			KBucketEntry entry = entryByIp.get().b;
			
			// this might happen if
			// a) multiple nodes on a single IP -> ignore anything but the node we already have in the table
			// b) one node changes ports (broken NAT?) -> ignore until routing table entry times out
			if(entry.getAddress().getPort() != msg.getOrigin().getPort())
				return;
				
			
			if(!entry.getID().equals(id)) {
				// ID mismatch
				
				
				if(associatedCall.isPresent()) {
					/*
					 *  we are here because:
					 *  a) a node with that IP is in our routing table
					 *  b) port matches too
					 *  c) the message is a response (mtid-verified)
					 *  d) the ID does not match our routing table entry
					 * 
					 *  That means we are certain that the node either changed its node ID or does some ID-spoofing.
					 *  In either case we don't want it in our routing table
					 */
					
					DHT.logInfo("force-removing routing table entry "+entry+" because ID-change was detected; new ID:" + msg.getID());
					bucket.removeEntryIfBad(entry, true);
					
					// might be pollution attack, check other entries in the same bucket too in case random pings can't keep up with scrubbing.
					RPCServer srv = msg.getServer();
					if(srv != null) {
						PingRefreshTask t = new PingRefreshTask(srv, this, null, false);
						if(maintenanceTasks.putIfAbsent(bucket, t) == null) {
							t.addListener(x -> maintenanceTasks.remove(bucket, t));
							t.checkGoodEntries(true);
							t.addBucket(bucket);
							// home bucket or something close to it, check adjacent buckets too
							if(bucket.getNumEntries() < DHTConstants.MAX_ENTRIES_PER_BUCKET) {
								List<RoutingTableEntry> table = routingTableCOW;
								int idx = Node.findIdxForId(table, entry.getID());
								t.addBucket(table.get(max(0,idx - 1)).getBucket());
								t.addBucket(table.get(min(table.size()-1,idx+1)).getBucket());
							}
							t.setInfo("checking sibling bucket entries after ID change was detected");
							dht.getTaskManager().addTask(t);
							
						}
					}
				}
				
				// even if this is not a response we don't want inmsert this in the routing table, it's an ID mismatch after all
				return;
			}
			
			
		}
		
		KBucket bucketById = findBucketForId(id).bucket;
		Optional<KBucketEntry> entryById = bucketById.findByIPorID(null, id);
		
		// entry is claiming the same ID as entry with different IP in our routing table -> ignore
		if(entryById.isPresent() && !entryById.get().getAddress().getAddress().equals(ip))
			return;
		
		// ID mismatch from call (not the same as ID mismatch from routing table)
		// it's fishy at least. don't insert even if it proves useful during a lookup
		if(!entryById.isPresent() && expectedId.isPresent() && !expectedId.get().equals(id))
			return;

		/*
			DHT.logInfo("response "+msg+" did not match expected ID, ignoring for the purpose routing table maintenance (may still be consumed by lookups)");
		};*/
			
		
		KBucketEntry newEntry = new KBucketEntry(msg.getOrigin(), id);
		newEntry.setVersion(msg.getVersion());
		associatedCall.ifPresent(c -> {
			newEntry.signalResponse(c.getRTT());
			// fudge it. it's not actually the sent time, but it's a successful response, so who cares
			newEntry.signalScheduledRequest();
		});
		
		/*
		if(scanForMismatchedEntry(newEntry)) {
			DHT.logInfo("ID or IP mismatching pre-existing routing table entries detected "+msg+"; ignoring for routing table maintenance");
			return;
		}*/
		
		// force trusted entry into the routing table (by splitting if necessary) if it passed all preliminary tests and it's not yet in the table
		// although we can only trust responses, anything else might be spoofed to clobber our routing table
		boolean trustedAndNotPresent = !entryById.isPresent() && msg.getType() == Type.RSP_MSG && trustedNodes.stream().anyMatch(mask -> mask.contains(ip));
			
		insertEntry(newEntry, false, trustedAndNotPresent, msg.getType() == Type.RSP_MSG);
		
		// we already should have the bucket. might be an old one by now due to splitting
		// but it doesn't matter, we just need to update the entry, which should stay the same object across bucket splits
		if(msg.getType() == Type.RSP_MSG) {
			bucketById.notifyOfResponse(msg);
		}
			
		
		num_receives++;
	}
	
	private Optional<Pair<KBucket, KBucketEntry>> bucketForIP(InetAddress addr) {
		return Optional.ofNullable(knownNodes.get(addr)).map(RoutingTableEntry::getBucket).flatMap(bucket -> bucket.findByIPorID(addr, null).map(Pair.of(bucket)));
	}
	
	/**
	 * Detects node ID or IP changes. such changes are ignored until the existing entry times out
	 * 
	 * @return true if there is a routing table entry that matches IP or ID of the new entry but is not equal to the new entry.
	 
	private boolean scanForMismatchedEntry(KBucketEntry newEntry) {
		
		// if either of them exists and doesn't match exactly ignore this node
		
		Optional<RoutingTableEntry> cachedEntry = Optional.ofNullable(knownNodes.get(newEntry.getAddress().getAddress()));
		Optional<KBucketEntry> existing = cachedEntry.flatMap(e -> e.bucket.findByIPorID(newEntry));
		if(existing.isPresent() && !existing.get().equals(newEntry))
			return true;
			
		existing = findBucketForId(newEntry.getID()).bucket.findByIPorID(newEntry);
		if(existing.isPresent() && !existing.get().equals(newEntry))
			return true;

		
		return false;
	}*/
	
	public void insertEntry(KBucketEntry entry, boolean internalInsert) {
		insertEntry(entry, internalInsert, false, false);
	}
	
	
	void insertEntry (KBucketEntry toInsert, boolean internalInsert, boolean isTrusted, boolean isResponse) {
		if(toInsert == null || usedIDs.contains(toInsert.getID()) || AddressUtils.isBogon(toInsert.getAddress()) || !dht.getType().PREFERRED_ADDRESS_TYPE.isInstance(toInsert.getAddress().getAddress()))
			return;
		
		Key nodeID = toInsert.getID();
		
		RoutingTableEntry tableEntry = findBucketForId(nodeID);
		while(tableEntry.bucket.getNumEntries() >= DHTConstants.MAX_ENTRIES_PER_BUCKET && tableEntry.prefix.getDepth() < Key.KEY_BITS - 1)
		{
			if(!canSplit(tableEntry, toInsert, !internalInsert && isResponse))
				break;
			
			splitEntry(tableEntry);
			tableEntry = findBucketForId(nodeID);
		}
		
		int oldSize = tableEntry.bucket.getNumEntries();
		
		KBucketEntry toRemove = null;
		
		if(isTrusted) {
			toRemove = tableEntry.bucket.getEntries().stream().filter(e -> trustedNodes.stream().noneMatch(mask -> mask.contains(e.getAddress().getAddress()))).max(KBucketEntry.AGE_ORDER).orElse(null);
		}
		
		if(internalInsert || isTrusted)
			tableEntry.bucket.modifyMainBucket(toRemove,toInsert);
		else
			tableEntry.bucket.insertOrRefresh(toInsert);
		
		// add delta to the global counter. inaccurate, but will be rebuilt by the bucket checks
		num_entries += tableEntry.bucket.getNumEntries() - oldSize;
		
	}
	
	boolean canSplit(RoutingTableEntry entry, KBucketEntry toInsert, boolean relaxedSplitting) {
		if(usedIDs.stream().anyMatch(localId -> entry.prefix.isPrefixOf(localId)))
			return true;
		
		if(!relaxedSplitting)
			return false;
		
		Comparator<Key> comp = new Key.DistanceOrder(toInsert.getID());
		
		Key closestLocalId = usedIDs.stream().min(comp).orElseThrow(() -> new IllegalStateException("expected to find a local ID"));
		
		List<RoutingTableEntry> table = routingTableCOW;
		
		int closer = 0;
		
		int center = findIdxForId(table, closestLocalId);
		
		for(int i=center;i<table.size();i++) {
			KBucket bucket = table.get(i).bucket;
			if(bucket.getNumEntries() == 0)
				continue;
			int found = (int) bucket.entriesStream().filter(e -> closestLocalId.threeWayDistance(e.getID(), toInsert.getID()) < 0).count();
			if(found == 0)
				break;
			closer+=found;
			if(closer >= DHTConstants.MAX_ENTRIES_PER_BUCKET)
				return false;
		}
		
		for(int i=center-1;i>=0;i--) {
			KBucket bucket = table.get(i).bucket;
			if(bucket.getNumEntries() == 0)
				continue;
			int found = (int) bucket.entriesStream().filter(e -> closestLocalId.threeWayDistance(e.getID(), toInsert.getID()) < 0).count();
			if(found == 0)
				break;
			closer+=found;
			if(closer >= DHTConstants.MAX_ENTRIES_PER_BUCKET)
				return false;
		}
		
		return closer < DHTConstants.MAX_ENTRIES_PER_BUCKET;
	}
	
	private void splitEntry(RoutingTableEntry entry) {
		synchronized (CoWLock)
		{
			List<RoutingTableEntry> newTable = new ArrayList<Node.RoutingTableEntry>(routingTableCOW);
			// check if we haven't entered the sync block after some other thread that did the same split operation
			if(!newTable.contains(entry))
				return;
			
			newTable.remove(entry);
			newTable.add(new RoutingTableEntry(entry.prefix.splitPrefixBranch(false), new KBucket(this)));
			newTable.add(new RoutingTableEntry(entry.prefix.splitPrefixBranch(true), new KBucket(this)));
			Collections.sort(newTable);
			routingTableCOW = newTable;
			for(KBucketEntry e : entry.bucket.getEntries())
				insertEntry(e, true);
			for(KBucketEntry e : entry.bucket.getReplacementEntries())
				insertEntry(e, true);
		}
		
	}
	
	public static int findIdxForId(List<RoutingTableEntry> table, Key id) {
        int lowerBound = 0;
        int upperBound = table.size()-1;

        while (lowerBound <= upperBound) {
            int pivotIdx = (lowerBound + upperBound) >>> 1;
            Prefix pivot = table.get(pivotIdx).prefix;

            if(pivot.isPrefixOf(id))
            	return pivotIdx;

            if (pivot.compareTo(id) < 0)
           		lowerBound = pivotIdx + 1;
           	else
           		upperBound = pivotIdx - 1;
        }
        throw new IllegalStateException("This shouldn't happen, really");
	}
	
	public RoutingTableEntry findBucketForId(Key id) {
		List<RoutingTableEntry> table = routingTableCOW;
		return table.get(findIdxForId(table, id));
	}

	/**
	 * @return OurID
	 */
	public Key getRootID () {
		if(dataStore != null)
			return (Key)dataStore.get("commonKey");
		// return a fake key if we're not initialized yet
		return Key.MIN_KEY;
	}
	
	public boolean isLocalId(Key id) {
		return usedIDs.contains(id);
	}
	
	public Collection<Key> localIDs() {
		return usedIDs.snapshot();
	}
	
	public DHT getDHT() {
		return dht;
	}

	/**
	 * Increase the failed queries count of the bucket entry we sent the message to
	*/
	void onTimeout (RPCCall call) {
		// don't timeout anything if we don't have a connection
		if(isInSurvivalMode())
			return;
		if(!call.getRequest().getServer().isReachable())
			return;
		
		InetSocketAddress dest = call.getRequest().getDestination();
		
		if(call.getExpectedID() != null)
		{
			findBucketForId(call.getExpectedID()).bucket.onTimeout(dest);
		} else {
			RoutingTableEntry entry = knownNodes.get(dest.getAddress());
			if(entry != null)
				entry.bucket.onTimeout(dest);
		}
			
	}
	
	public boolean isInSurvivalMode() {
		return dht.getServerManager().getActiveServerCount() == 0;
	}
	
	void removeId(Key k)
	{
		usedIDs.remove(k);
	}
	
	void registerServer(RPCServer srv) {
		srv.onEnqueue(this::onOutgoingRequest);
	}
	
	private void onOutgoingRequest(RPCCall c) {
		Key expectedId = c.getExpectedID();
		KBucket bucket = findBucketForId(expectedId).getBucket();
		bucket.findByIPorID(c.getRequest().getDestination().getAddress(), expectedId).ifPresent(entry -> {
			entry.signalScheduledRequest();
		});
		
	}
	
	Key registerId()
	{
		int idx = 0;
		Key k = null;
		
		while(true)
		{
			k = getRootID().getDerivedKey(idx);
			if(usedIDs.add(k))
				break;
			idx++;
		}

		return k;
	}
	
	
	

	/**
	 * Check if a buckets needs to be refreshed, and refresh if necessary.
	 */
	public void doBucketChecks (long now) {
		
		
		// don't spam the checks if we're not receiving anything.
		// we don't want to cause too many stray packets somewhere in a network
		if(isInSurvivalMode() && now - timeOfLastPingCheck > DHTConstants.BOOTSTRAP_MIN_INTERVAL)
			return;
		timeOfLastPingCheck = now;

		synchronized (CoWLock)
		{
			// perform bucket merge operations where possible
			for(int i=1;i<routingTableCOW.size();i++)
			{
				RoutingTableEntry e1 = routingTableCOW.get(i-1);
				RoutingTableEntry e2 = routingTableCOW.get(i);

				if(e1.prefix.isSiblingOf(e2.prefix))
				{
					// uplift siblings if the other one is dead
					if(e1.getBucket().getNumEntries() == 0)
					{
						List<RoutingTableEntry> newTable = new ArrayList<Node.RoutingTableEntry>(routingTableCOW);
						newTable.remove(e1);
						newTable.remove(e2);
						newTable.add(new RoutingTableEntry(e2.prefix.getParentPrefix(), e2.getBucket()));
						Collections.sort(newTable);
						routingTableCOW = newTable;
						i--;continue;
					}

					if(e2.getBucket().getNumEntries() == 0)
					{
						List<RoutingTableEntry> newTable = new ArrayList<Node.RoutingTableEntry>(routingTableCOW);
						newTable.remove(e1);
						newTable.remove(e2);
						newTable.add(new RoutingTableEntry(e1.prefix.getParentPrefix(), e1.getBucket()));
						Collections.sort(newTable);
						routingTableCOW = newTable;
						i--;continue;

					}
					
					// check if the buckets can be merged without losing entries
					if(e1.getBucket().getNumEntries() + e2.getBucket().getNumEntries() < DHTConstants.MAX_ENTRIES_PER_BUCKET)
					{
						List<RoutingTableEntry> newTable = new ArrayList<Node.RoutingTableEntry>(routingTableCOW);
						newTable.remove(e1);
						newTable.remove(e2);
						newTable.add(new RoutingTableEntry(e1.prefix.getParentPrefix(), new KBucket(this)));
						Collections.sort(newTable);
						routingTableCOW = newTable;
						// no need to carry over replacements. there shouldn't be any, otherwise the bucket(s) would be full
						for(KBucketEntry e : e1.bucket.getEntries())
							insertEntry(e, true);
						for(KBucketEntry e : e2.bucket.getEntries())
							insertEntry(e, true);
						i--;continue;
					}
				}

			}

		}
		
		int newEntryCount = 0;
		
		for (RoutingTableEntry e : routingTableCOW) {
			KBucket b = e.bucket;

			List<KBucketEntry> entries = b.getEntries();
			
			Set<Key> localIds = usedIDs.snapshot();

			// remove boostrap nodes from our buckets
			boolean wasFull = b.getNumEntries() >= DHTConstants.MAX_ENTRIES_PER_BUCKET;
			boolean allBad = true;
			for (KBucketEntry entry : entries)
			{
				if (wasFull && DHTConstants.BOOTSTRAP_NODE_ADDRESSES.contains(entry.getAddress()))
					b.removeEntryIfBad(entry, true);
				if(localIds.contains(entry.getID()))
					b.removeEntryIfBad(entry, true);
				allBad &= entry.needsReplacement();
				
				
			}

			// clean out buckets full of bad nodes. merge operations will do the rest
			if(!isInSurvivalMode() && allBad)
			{
				e.bucket = new KBucket(this);
				continue;
			}
				
			
			if (!maintenanceTasks.containsKey(b) && b.needsToBeRefreshed())
			{
				
				RPCServer srv = dht.getServerManager().getRandomActiveServer(true);
				if(srv != null) {
					PingRefreshTask prt = new PingRefreshTask(srv, this, b, false);
					
					prt.setInfo("Refreshing Bucket #" + e.prefix);
					
					if(prt.getTodoCount() > 0 && maintenanceTasks.putIfAbsent(b, prt) == null) {
						prt.addListener(x -> maintenanceTasks.remove(b, prt));
						dht.getTaskManager().addTask(prt);
					}
						
				}
					


			}
			
			if(!isInSurvivalMode())	{
				// only replace 1 bad entry with a replacement bucket entry at a time (per bucket)
				b.checkBadEntries();
			}
			
			newEntryCount += e.bucket.getNumEntries();


		}
		
		num_entries = newEntryCount;
		
		rebuildAddressCache();
	}
	
	void rebuildAddressCache() {
		Map<InetAddress, RoutingTableEntry> newKnownMap = new HashMap<InetAddress, RoutingTableEntry>(num_entries);
		List<RoutingTableEntry> table = routingTableCOW;
		for(int i=0,n=table.size();i<n;i++)
		{
			RoutingTableEntry entry = table.get(i);
			Stream<KBucketEntry> entries = entry.bucket.entriesStream();
			entries.forEach(e -> {
				newKnownMap.put(e.getAddress().getAddress(), entry);
			});
		}
		
		knownNodes = newKnownMap;
	}

	/**
	 * Check if a buckets needs to be refreshed, and refresh if necesarry
	 *
	 * @param dh_table
	 */
	public void fillBuckets (DHTBase dh_table) {
		List<RoutingTableEntry> table = routingTableCOW;

		for (int i = 0;i<table.size();i++) {
			RoutingTableEntry entry = table.get(i);

			if (entry.bucket.getNumEntries() < DHTConstants.MAX_ENTRIES_PER_BUCKET) {

				NodeLookup nl = dh_table.fillBucket(entry.prefix.createRandomKeyFromPrefix(), entry.bucket);
				if (nl != null) {
					nl.setInfo("Filling Bucket #" + entry.prefix);
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
	void saveTable(File file) throws IOException {
		
		
		Path saveTo = file.toPath();
		
		Path tempFile = Files.createTempFile(saveTo.getParent(), "saveTable", "tmp");
		
		try(ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(tempFile, StandardOpenOption.WRITE, StandardOpenOption.SYNC), 512*1024))) {
			HashMap<String,Serializable> tableMap = new HashMap<String, Serializable>();
			
			dataStore.put("table"+dht.getType().name(), tableMap);
			
			tableMap.put("oldKey", getRootID());
			
			List<RoutingTableEntry> table = routingTableCOW;
			
			KBucket[] bucket = new KBucket[table.size()];
			for(int i=0;i<bucket.length;i++)
				bucket[i] = table.get(i).bucket;
				
			tableMap.put("bucket", bucket);
			tableMap.put("log2estimate", dht.getEstimator().getRawDistanceEstimate());
			tableMap.put("timestamp", System.currentTimeMillis());
			
			oos.writeObject(dataStore);
			oos.close();
			
			Files.move(tempFile, saveTo, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		}
	}
	
	synchronized static void initDataStore(DHTConfiguration config)
	{
		File file = config.getNodeCachePath();
		
		if(dataStore != null)
			return;
		
		if (file.exists()) {
			try (FileInputStream fis = new FileInputStream(file); ObjectInputStream ois = new ObjectInputStream(fis)) {
				dataStore = (Map<String, Serializable>) ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if(dataStore == null)
		{
			dataStore = new ConcurrentSkipListMap<>();
			dataStore.put("commonKey", Key.createRandomKey());
		}
		
		if(!config.isPersistingID())
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
	void loadTable () {

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
			
			for(int i=0;i<loadedBuckets.length;i++)
			{
				KBucket b = loadedBuckets[i];
				if(b == null)
					continue;
				entriesLoaded += b.getNumEntries();
				entriesLoaded += b.getReplacementEntries().size();
				for(KBucketEntry e : b.getEntries())
					insertEntry(e,true);
				for(KBucketEntry e : b.getReplacementEntries())
					insertEntry(e,true);
			}
			
			rebuildAddressCache();

			DHT.logInfo("Loaded " + entriesLoaded + " from cache. Cache was "
					+ ((System.currentTimeMillis() - timestamp) / (60 * 1000))
					+ "min old. Reusing old id = " + oldID.equals(getRootID()));

			return;
		} catch (Exception e) {
			// loading the cache can fail for various reasons... just log and bootstrap if we have to
			DHT.log(e,LogLevel.Error);
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

	public List<RoutingTableEntry> getBuckets () {
		return Collections.unmodifiableList(routingTableCOW) ;
	}
	
	public void setTrustedNetMasks(Collection<NetMask> masks) {
		trustedNodes = masks;
	}
	
	public Collection<NetMask> getTrustedNetMasks() {
		return trustedNodes;
	}
	
	public Optional<KBucketEntry> getRandomEntry() {
		List<RoutingTableEntry> table = routingTableCOW;
		
		int offset = ThreadLocalRandom.current().nextInt(table.size());
		
		// sweep from a random offset in case there are empty buckets
		return IntStream.range(0, table.size()).mapToObj(i -> table.get((i + offset) % table.size()).getBucket().randomEntry()).filter(Optional::isPresent).map(Optional::get).findAny();
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder(10000);
		List<RoutingTableEntry> table = routingTableCOW;
		
		Collection<Key> localIds = localIDs();
		
		b.append("buckets: ").append(table.size()).append(" / entries: ").append(num_entries).append('\n');
		for(RoutingTableEntry e : table ) {
			b.append(e.prefix).append("   num:").append(e.bucket.getNumEntries()).append(" rep:").append(e.bucket.getNumReplacements());
			if(localIds.stream().anyMatch(e.prefix::isPrefixOf))
				b.append(" [Home]");
			b.append('\n');
		}
			
		return b.toString();
	}

}
