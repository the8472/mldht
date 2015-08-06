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
import java.util.Arrays;
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
import java.util.function.Consumer;
import java.util.function.Predicate;
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
	/*
	 * Verification Strategy:
	 * 
	 * - trust incoming requests less than responses to outgoing requests
	 * - most outgoing requests will have an expected ID - expected ID may come from external nodes, so don't take it at face value
	 *  - if response does not match expected ID drop the packet for routing table accounting purposes without penalizing any existing routing table entry
	 * - map routing table entries to IP addresses
	 *  - verified responses trump unverified entries
	 *  - lookup all routing table entry for incoming messages based on IP address (not node ID!) and ignore them if ID does not match
	 *  - also ignore if port changed
	 *  - drop, not just ignore, if we are sure that the incoming message is not fake (mtid-verified response)
	 * - allow duplicate addresses for unverified entries
	 *  - scrub later when one becomes verified
	 * - never hand out unverified entries to other nodes
	 * 
	 */
	
	public static final class RoutingTableEntry implements Comparable<RoutingTableEntry> {
		
		public RoutingTableEntry(Prefix prefix, KBucket bucket, Predicate<Prefix> checkHome) {
			this.prefix = prefix;
			this.bucket = bucket;
			this.homeBucket = checkHome.test(prefix);
		}
		
		public final Prefix prefix;
		final KBucket bucket;
		final boolean homeBucket;
		
		public KBucket getBucket() {
			return bucket;
		}
		
		public int compareTo(RoutingTableEntry o) {
			return prefix.compareTo(o.prefix);
		}
		
		@Override
		public String toString() {
			return prefix.toString() + " " + bucket.toString();
		}
	}
	
	public static final class RoutingTable {
		
		final RoutingTableEntry[] entries;
		final int[] indexCache;
		
		RoutingTable(RoutingTableEntry... entries) {
			this.entries = entries;
			if(entries.length > 64) {
				indexCache = buildCache();
			} else {
				indexCache = new int[] {0, entries.length};
			}
			
		}
		
		public RoutingTable() {
			this(new RoutingTableEntry[] {new RoutingTableEntry(new Prefix(), new KBucket(), (x) -> true)});
		}
		
		int[] buildCache() {
			int[] cache = new int[256];
			
			assert(Integer.bitCount(cache.length) == 1);
			
			int lsb = Integer.bitCount((cache.length/2)-1)-1;
			
			Key increment = Key.setBit(lsb);
			Key trailingBits = new Prefix(Key.MAX_KEY, lsb).distance(Key.MAX_KEY);
			Key currentLower = new Key(new Prefix(Key.MIN_KEY, lsb));
			Key currentUpper = new Prefix(Key.MIN_KEY, lsb).distance(trailingBits);
			
			int innerOffset = 0;
			
			for(int i=0;i<cache.length;i+=2) {
				cache[i+1] = entries.length;
				
				for(int j=innerOffset;j<entries.length;j++) {
					Prefix p = entries[j].prefix;
					
					if(p.compareTo(currentLower) <= 0) {
						innerOffset = cache[i] = max(cache[i],j);
					}
						
					if(p.compareTo(currentUpper) >= 0) {
						cache[i+1] = min(cache[i+1],j);
						break;
					}
						
				}
				
				currentLower = new Key(new Prefix(currentLower.add(increment), lsb));
				currentUpper = currentLower.distance(trailingBits);
			}
			
			// System.out.println(IntStream.of(cache).mapToObj(Integer::toString).collect(Collectors.joining(", ")));
			
			return cache;
		}
		
		public int indexForId(Key id) {
			int mask = indexCache.length/2 - 1;
			int bits = Integer.bitCount(mask);
			
			int cacheIdx = id.getInt(0);
			
			cacheIdx = Integer.rotateLeft(cacheIdx, bits);
			cacheIdx = cacheIdx & mask;
			cacheIdx <<= 1;
			
	        int lowerBound = indexCache[cacheIdx];
	        int upperBound = indexCache[cacheIdx+1];
	        
	        Prefix pivot = null;

	        while(true) {
	            int pivotIdx = (lowerBound + upperBound) >>> 1;
	            pivot = entries[pivotIdx].prefix;
	            
	            if(pivotIdx == lowerBound)
	            	break;

	            if (pivot.compareTo(id) <= 0)
	           		lowerBound = pivotIdx;
	           	else
	           		upperBound = pivotIdx;
	        }
	        
	        assert(pivot != null && pivot.isPrefixOf(id));
	        
           	return lowerBound;
		}

		
		public RoutingTableEntry entryForId(Key id) {
			return entries[indexForId(id)];
		}
		
		public int size() {
			return entries.length;
		}
		
		public RoutingTableEntry get(int idx) {
			return entries[idx];
		}
		
		public List<RoutingTableEntry> list() {
			return Collections.unmodifiableList(Arrays.asList(entries));
		}
		
		public Stream<RoutingTableEntry> stream() {
			return Arrays.stream(entries);
		}
		
		public RoutingTable modify(Collection<RoutingTableEntry> toRemove, Collection<RoutingTableEntry> toAdd) {
			List<RoutingTableEntry> temp = new ArrayList<>(Arrays.asList(entries));
			if(toRemove != null)
				temp.removeAll(toRemove);
			if(toAdd != null)
				temp.addAll(toAdd);
			return new RoutingTable(temp.stream().sorted().toArray(RoutingTableEntry[]::new));
		}
		
	}

	private Object CoWLock = new Object();
	private volatile RoutingTable routingTableCOW = new RoutingTable();
	
	
	
	
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
			KBucket oldBucket = entryByIp.get().a;
			KBucketEntry oldEntry = entryByIp.get().b;
			
			// this might happen if
			// a) multiple nodes on a single IP -> ignore anything but the node we already have in the table
			// b) one node changes ports (broken NAT?) -> ignore until routing table entry times out
			if(oldEntry.getAddress().getPort() != msg.getOrigin().getPort())
				return;
				
			
			if(!oldEntry.getID().equals(id)) { // ID mismatch
				
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
					
					DHT.logInfo("force-removing routing table entry "+oldEntry+" because ID-change was detected; new ID:" + msg.getID());
					oldBucket.removeEntryIfBad(oldEntry, true);
					
					// might be pollution attack, check other entries in the same bucket too in case random pings can't keep up with scrubbing.
					RPCServer srv = msg.getServer();
					tryPingMaintenance(oldBucket, "checking sibling bucket entries after ID change was detected", srv, (t) -> t.checkGoodEntries(true));
					
					if(oldEntry.verifiedReachable()) {
						// old verified
						// new verified
						// -> probably misbehaving node. don't insert
						return;
					}
					
					/*
					 *  old never verified
					 *  new verified
					 *  -> may allow insert, as if the old one has never been there
					 * 
					 *  but still need to check expected ID match.
					 *  TODO: if this results in an insert then the known nodes list may be stale
					 */
					
				} else {
					
					// new message is *not* a response -> not verified -> fishy due to ID mismatch -> ignore
					return;
				}
				
			}
			
			
		}
		
		KBucket bucketById = routingTableCOW.entryForId(id).bucket;
		Optional<KBucketEntry> entryById = bucketById.findByIPorID(null, id);
		
		// entry is claiming the same ID as entry with different IP in our routing table -> ignore
		if(entryById.isPresent() && !entryById.get().getAddress().getAddress().equals(ip))
			return;
		
		// ID mismatch from call (not the same as ID mismatch from routing table)
		// it's fishy at least. don't insert even if it proves useful during a lookup
		if(!entryById.isPresent() && expectedId.isPresent() && !expectedId.get().equals(id))
			return;

		KBucketEntry newEntry = new KBucketEntry(msg.getOrigin(), id);
		newEntry.setVersion(msg.getVersion());
		associatedCall.ifPresent(c -> {
			newEntry.signalResponse(c.getRTT());
			// fudge it. it's not actually the sent time, but it's a successful response, so who cares
			newEntry.signalScheduledRequest();
		});
		

		
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
	
	
	public void insertEntry(KBucketEntry entry, boolean internalInsert) {
		insertEntry(entry, internalInsert, false, false);
	}
	
	
	void insertEntry (KBucketEntry toInsert, boolean internalInsert, boolean isTrusted, boolean isResponse) {
		if(toInsert == null || usedIDs.contains(toInsert.getID()) || AddressUtils.isBogon(toInsert.getAddress()) || !dht.getType().PREFERRED_ADDRESS_TYPE.isInstance(toInsert.getAddress().getAddress()))
			return;
		
		Key nodeID = toInsert.getID();
		
		RoutingTableEntry tableEntry = routingTableCOW.entryForId(nodeID);

		while(tableEntry.bucket.getNumEntries() >= DHTConstants.MAX_ENTRIES_PER_BUCKET && tableEntry.prefix.getDepth() < Key.KEY_BITS - 1)
		{
			if(!canSplit(tableEntry, toInsert, !internalInsert && isResponse))
				break;
			
			splitEntry(tableEntry);
			tableEntry = routingTableCOW.entryForId(nodeID);
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
		if(entry.homeBucket)
			return true;
		
		if(!relaxedSplitting)
			return false;
		
		Comparator<Key> comp = new Key.DistanceOrder(toInsert.getID());
		
		Key closestLocalId = usedIDs.stream().min(comp).orElseThrow(() -> new IllegalStateException("expected to find a local ID"));
		
		KClosestNodesSearch search = new KClosestNodesSearch(closestLocalId, DHTConstants.MAX_ENTRIES_PER_BUCKET, dht);
		
		search.filter = x -> true;
		
		search.fill();
		List<KBucketEntry> found = search.getEntries();
		
		if(found.size() < DHTConstants.MAX_ENTRIES_PER_BUCKET)
			return true;
		
		KBucketEntry max = found.get(found.size()-1);
		
		return closestLocalId.threeWayDistance(max.getID(), toInsert.getID()) > 0;
	}
	
	private void splitEntry(RoutingTableEntry entry) {
		synchronized (CoWLock)
		{
			RoutingTable current = routingTableCOW;
			// check if we haven't entered the sync block after some other thread that did the same split operation
			if(current.stream().noneMatch(e -> e.equals(entry)))
				return;
			
			RoutingTableEntry a = new RoutingTableEntry(entry.prefix.splitPrefixBranch(false), new KBucket(), this::isLocalBucket);
			RoutingTableEntry b = new RoutingTableEntry(entry.prefix.splitPrefixBranch(true), new KBucket(), this::isLocalBucket);
			
			RoutingTable newTable = current.modify(Arrays.asList(entry), Arrays.asList(a, b));
			
			routingTableCOW = newTable;
			for(KBucketEntry e : entry.bucket.getEntries())
				insertEntry(e, true);
			for(KBucketEntry e : entry.bucket.getReplacementEntries())
				insertEntry(e, true);
		}
		
	}
	
	public RoutingTable table() {
		return routingTableCOW;
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
	
	public boolean isLocalBucket(Prefix p) {
		return usedIDs.stream().anyMatch(p::isPrefixOf);
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
			routingTableCOW.entryForId(call.getExpectedID()).bucket.onTimeout(dest);
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
		updateHomeBuckets();
	}
	
	void registerServer(RPCServer srv) {
		srv.onEnqueue(this::onOutgoingRequest);
	}
	
	private void onOutgoingRequest(RPCCall c) {
		Key expectedId = c.getExpectedID();
		KBucket bucket = routingTableCOW.entryForId(expectedId).getBucket();
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
		
		updateHomeBuckets();

		return k;
	}
	
	
	

	/**
	 * Check if a buckets needs to be refreshed, and refresh if necessary.
	 */
	public void doBucketChecks (long now) {
		
		boolean survival = isInSurvivalMode();
		
		// don't spam the checks if we're not receiving anything.
		// we don't want to cause too many stray packets somewhere in a network
		if(survival && now - timeOfLastPingCheck > DHTConstants.BOOTSTRAP_MIN_INTERVAL)
			return;
		timeOfLastPingCheck = now;
		
		mergeBuckets();
		
		int newEntryCount = 0;
		
		Map<InetAddress, KBucket> addressDedup = new HashMap<>(num_entries);
		
		for (RoutingTableEntry e : routingTableCOW.entries) {
			KBucket b = e.bucket;

			List<KBucketEntry> entries = b.getEntries();
			
			Set<Key> localIds = usedIDs.snapshot();

			boolean wasFull = b.getNumEntries() >= DHTConstants.MAX_ENTRIES_PER_BUCKET;
			for (KBucketEntry entry : entries)
			{
				// remove really old entries, ourselves and bootstrap nodes if the bucket is full
				if ((!survival && entry.removableWithoutReplacement()) || localIds.contains(entry.getID()) || (wasFull && DHTConstants.BOOTSTRAP_NODE_ADDRESSES.contains(entry.getAddress()))) {
					b.removeEntryIfBad(entry, true);
					continue;
				}
				
				// remove duplicate addresses. prefer to keep reachable entries
				addressDedup.compute(entry.getAddress().getAddress(), (addr, oldBucket) -> {
					if(oldBucket != null) {
						KBucketEntry oldEntry = oldBucket.findByIPorID(addr, null).orElse(null);
						if(oldEntry != null) {
							if(oldEntry.verifiedReachable()) {
								b.removeEntryIfBad(entry, true);
								return oldBucket;
							} else if(entry.verifiedReachable()) {
								oldBucket.removeEntryIfBad(oldEntry, true);
							}
						}
					}
					return b;
				});
				
			}
			
			/*
			boolean allBad = b.getNumEntries() > 0 && b.entriesStream().allMatch(KBucketEntry::removableWithoutReplacement);


			// clean out buckets full of bad nodes. merge operations will do the rest
			if(!survival && allBad)
			{
				clearEntry(e);
				continue;
			}*/
				
			
			if (b.needsToBeRefreshed())
				tryPingMaintenance(b, "Refreshing Bucket #" + e.prefix);
			
			if(!survival)	{
				// only replace 1 bad entry with a replacement bucket entry at a time (per bucket)
				b.checkBadEntries();
			}
			
			newEntryCount += e.bucket.getNumEntries();


		}
		
		num_entries = newEntryCount;
		
		rebuildAddressCache();
	}

	void tryPingMaintenance(KBucket b, String reason, RPCServer srv, Consumer<PingRefreshTask> taskConfig) {
		if(maintenanceTasks.containsKey(b))
			return;
		
		
		if(srv != null) {
			PingRefreshTask prt = new PingRefreshTask(srv, this, null, false);
			
			if(taskConfig != null)
				taskConfig.accept(prt);
			prt.setInfo(reason);
			
			prt.addBucket(b);
			
			if(prt.getTodoCount() > 0 && maintenanceTasks.putIfAbsent(b, prt) == null) {
				prt.addListener(x -> maintenanceTasks.remove(b, prt));
				dht.getTaskManager().addTask(prt);
			}
				
		}
	}
	
	void tryPingMaintenance(KBucket b, String reason) {
		RPCServer srv = dht.getServerManager().getRandomActiveServer(true);
		tryPingMaintenance(b, reason, srv, null);
	}
	
	
	
	
	void mergeBuckets() {
		synchronized (CoWLock) {
			
			// perform bucket merge operations where possible
			for (int i = 1; i < routingTableCOW.size(); i++) {
				if(i < 1)
					continue;
				RoutingTableEntry e1 = routingTableCOW.get(i - 1);
				RoutingTableEntry e2 = routingTableCOW.get(i);

				if (e1.prefix.isSiblingOf(e2.prefix)) {
					// uplift siblings if the other one is dead
					if (e1.getBucket().getNumEntries() == 0 || e2.getBucket().getNumEntries() == 0) {
						KBucket toLift = e1.getBucket().getNumEntries() == 0 ? e2.getBucket() : e1.getBucket();

						RoutingTable table = routingTableCOW;
						routingTableCOW = table.modify(Arrays.asList(e1, e2), Arrays.asList(new RoutingTableEntry(e2.prefix.getParentPrefix(), toLift, this::isLocalBucket)));
						i -= 2;
						continue;
					}

					// check if the buckets can be merged without losing entries

					if (e1.getBucket().getNumEntries() + e2.getBucket().getNumEntries() <= DHTConstants.MAX_ENTRIES_PER_BUCKET) {

						RoutingTable table = routingTableCOW;
						routingTableCOW = table.modify(Arrays.asList(e1, e2), Arrays.asList(new RoutingTableEntry(e1.prefix.getParentPrefix(), new KBucket(), this::isLocalBucket)));
						// no need to carry over replacements. there shouldn't
						// be any, otherwise the bucket(s) would be full
						for (KBucketEntry e : e1.bucket.getEntries())
							insertEntry(e, true);
						for (KBucketEntry e : e2.bucket.getEntries())
							insertEntry(e, true);
						i -= 2;
						continue;
					}
				}
				


			}
		}
	}
	
	void updateHomeBuckets() {
		synchronized (CoWLock) {
			for(int i=0;i < routingTableCOW.size();i++) {
				RoutingTableEntry e = routingTableCOW.get(i);
				// update home bucket status on local ID change
				if(isLocalBucket(e.prefix) != e.homeBucket)
					routingTableCOW = routingTableCOW.modify(Arrays.asList(e), Arrays.asList(new RoutingTableEntry(e.prefix, e.bucket, this::isLocalBucket)));
			}
		}
	}
	
	void rebuildAddressCache() {
		Map<InetAddress, RoutingTableEntry> newKnownMap = new HashMap<InetAddress, RoutingTableEntry>(num_entries);
		RoutingTable table = routingTableCOW;
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
		RoutingTable table = routingTableCOW;

		for (int i = 0;i<table.size();i++) {
			RoutingTableEntry entry = table.get(i);
			
			int num = entry.bucket.getNumEntries();

			// just try to fill partially populated buckets
			// not empty ones, they may arise as artifacts from deep splitting
			if (num > 0 && num < DHTConstants.MAX_ENTRIES_PER_BUCKET) {

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
			
			RoutingTable table = routingTableCOW;
			
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
	
	public void setTrustedNetMasks(Collection<NetMask> masks) {
		trustedNodes = masks;
	}
	
	public Collection<NetMask> getTrustedNetMasks() {
		return trustedNodes;
	}
	
	public Optional<KBucketEntry> getRandomEntry() {
		RoutingTable table = routingTableCOW;
		
		int offset = ThreadLocalRandom.current().nextInt(table.size());
		
		// sweep from a random offset in case there are empty buckets
		return IntStream.range(0, table.size()).mapToObj(i -> table.get((i + offset) % table.size()).getBucket().randomEntry()).filter(Optional::isPresent).map(Optional::get).findAny();
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder(10000);
		RoutingTable table = routingTableCOW;
		
		Collection<Key> localIds = localIDs();
		
		b.append("buckets: ").append(table.size()).append(" / entries: ").append(num_entries).append('\n');
		for(RoutingTableEntry e : table.entries) {
			b.append(e.prefix).append("   num:").append(e.bucket.getNumEntries()).append(" rep:").append(e.bucket.getNumReplacements());
			if(localIds.stream().anyMatch(e.prefix::isPrefixOf))
				b.append(" [Home]");
			b.append('\n');
		}
			
		return b.toString();
	}

}
