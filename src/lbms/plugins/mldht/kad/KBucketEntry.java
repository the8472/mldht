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

import static the8472.bencode.Utils.prettyPrint;
import static the8472.utils.Functional.typedGet;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.utils.ExponentialWeightendMovingAverage;

/**
 * Entry in a KBucket, it basically contains an ip_address of a node,
 * the udp port of the node and a node_id.
 *
 * @author Damokles
 */
public class KBucketEntry {
	
	private static final double RTT_EMA_WEIGHT = 0.3;
	
	/**
	 * ascending order for last seen, i.e. the last value will be the least recently seen one
	 */
	public static final Comparator<KBucketEntry> LAST_SEEN_ORDER = (o1, o2) -> Long.signum(o1.lastSeen - o2.lastSeen);

	/**
	 * ascending order for timeCreated, i.e. the first value will be the oldest
	 */
	public static final Comparator<KBucketEntry> AGE_ORDER = (o1, o2) -> Long.signum(o1.timeCreated - o2.timeCreated);

	/**
	 * same order as the Key class, based on the Entrie's nodeID
	 */
	public static final Comparator<KBucketEntry> KEY_ORDER = (o1, o2) -> o1.nodeID.compareTo(o2.nodeID);

	
	public static final class DistanceOrder implements Comparator<KBucketEntry> {
		
		final Key target;
		public DistanceOrder(Key target) {
			this.target = target;
		}
	
		public int compare(KBucketEntry o1, KBucketEntry o2) {
			return target.threeWayDistance(o1.getID(), o2.getID());
		}
	}
	

	private final InetSocketAddress	addr;
	private final Key				nodeID;
	private long				lastSeen;
	private boolean verified = false;
	
	/**
	 *   -1 = never queried / learned about it from incoming requests
	 *    0 = last query was a success
	 *  > 0 = query failed
	 */
	private int					failedQueries;
	private long				timeCreated;
	private byte[]				version;
	private ExponentialWeightendMovingAverage avgRTT = new ExponentialWeightendMovingAverage().setWeight(RTT_EMA_WEIGHT);;
	private long lastSendTime = -1;

	
	public static KBucketEntry fromBencoded(Map<String, Object> serialized, DHTtype expectedType) {
		
		InetSocketAddress addr = typedGet(serialized, "addr", byte[].class).map(AddressUtils::unpackAddress).orElseThrow(() -> new IllegalArgumentException("address missing"));
		Key id = typedGet(serialized, "id", byte[].class).filter(b -> b.length == Key.SHA1_HASH_LENGTH).map(Key::new).orElseThrow(() -> new IllegalArgumentException("key missing"));
		
		KBucketEntry built = new KBucketEntry(addr, id);
		
		typedGet(serialized, "version", byte[].class).ifPresent(built::setVersion);
		typedGet(serialized, "created", Long.class).ifPresent(l -> built.timeCreated = l);
		typedGet(serialized, "lastSeen", Long.class).ifPresent(l -> built.lastSeen = l);
		typedGet(serialized, "lastSend", Long.class).ifPresent(l -> built.lastSendTime = l);
		typedGet(serialized, "failedCount", Long.class).ifPresent(l -> built.failedQueries = l.intValue());
		typedGet(serialized, "verified", Long.class).ifPresent(l -> built.setVerified(l == 1));
		
		
		return built;
	}
	
	public Map<String, Object> toBencoded() {
		Map<String, Object> map = new TreeMap<>();
		
		map.put("addr", AddressUtils.packAddress(addr));
		map.put("id", nodeID.getHash());
		map.put("created", timeCreated);
		map.put("lastSeen", lastSeen);
		map.put("lastSend", lastSendTime);
		map.put("failedCount", failedQueries());
		if(version != null)
			map.put("version", version);
		if(verifiedReachable())
			map.put("verified", 1);
		
		return map;
	}


	/**
	 * Constructor, set the ip, port and key
	 * @param addr socket address
	 * @param id ID of node
	 */
	public KBucketEntry (InetSocketAddress addr, Key id) {
		lastSeen = System.currentTimeMillis();
		timeCreated = lastSeen;
		this.addr = addr;
		this.nodeID = id;
	}

	/**
	 * Constructor, set the ip, port and key
	 * @param addr socket address
	 * @param id ID of node
	 * @param timestamp the timestamp when this node last responded
	 */
	public KBucketEntry (InetSocketAddress addr, Key id, long timestamp) {
		lastSeen = timestamp;
		timeCreated = System.currentTimeMillis();
		this.addr = addr;
		this.nodeID = id;
	}

	/**
	 * Copy constructor.
	 * @param other KBucketEntry to copy
	 * @return
	 */
	public KBucketEntry (KBucketEntry other) {
		addr = other.addr;
		nodeID = other.nodeID;
		lastSeen = other.lastSeen;
		failedQueries = other.failedQueries;
		timeCreated = other.timeCreated;
	}

	/**
	 * @return the address of the node
	 */
	public InetSocketAddress getAddress () {
		return addr;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(o instanceof KBucketEntry)
			return this.equals((KBucketEntry)o);
		return this == o;
	}

	public boolean equals(KBucketEntry other) {
		if(other == null)
			return false;
		return nodeID.equals(other.nodeID) && addr.equals(other.addr);
	}
	
	public boolean matchIPorID(KBucketEntry other) {
		if(other == null)
			return false;
		return nodeID.equals(other.getID()) || addr.getAddress().equals(other.addr.getAddress());
	}

	@Override
	public int hashCode () {
		return nodeID.hashCode() + 1;
	}

	/**
	 * @return id
	 */
	public Key getID () {
		return nodeID;
	}

	/**
     * @param version the version to set
     */
    public void setVersion (byte[] version) {
	    this.version = version;
    }

	/**
     * @return the version
     */
    public ByteBuffer getVersion () {
	    return ByteBuffer.wrap(version).asReadOnlyBuffer();
    }

	/**
	 * @return the last_responded
	 */
	public long getLastSeen () {
		return lastSeen;
	}

	public long getCreationTime() {
		return timeCreated;
	}

	/**
	 * @return the failedQueries
	 */
	public int getFailedQueries () {
		return failedQueries;
	}
	
	@Override
	public String toString() {
		long now = System.currentTimeMillis();
		StringBuilder b = new StringBuilder(80);
		b.append(nodeID+"/"+addr);
		if(lastSendTime > 0)
			b.append(";sent:"+Duration.ofMillis(now-lastSendTime));
		b.append(";seen:"+Duration.ofMillis(now-lastSeen));
		b.append(";age:"+Duration.ofMillis(now-timeCreated));
		if(failedQueries != 0)
			b.append(";fail:"+failedQueries);
		if(verified)
			b.append(";verified");
		double rtt = avgRTT.getAverage();
		if(!Double.isNaN(rtt))
			b.append(";rtt:"+rtt);
		if(version != null)
			b.append(";ver:"+prettyPrint(version));
			
		return b.toString();
	}




	// 5 timeouts, used for exponential backoff as per kademlia paper
	public static final int MAX_TIMEOUTS = 5;
	
	// haven't seen it for a long time + timeout == evict sooner than pure timeout based threshold. e.g. for old entries that we haven't touched for a long time
	public static final int OLD_AND_STALE_TIME = 15*60*1000;
	public static final int PING_BACKOFF_BASE_INTERVAL = 60*1000;
	public static final int OLD_AND_STALE_TIMEOUTS = 2;
	
	public boolean eligibleForNodesList() {
		// 1 timeout can occasionally happen. should be fine to hand it out as long as we've verified it at least once
		return verifiedReachable() && failedQueries < 2;
	}
	
	
	public boolean eligibleForLocalLookup() {
		// allow implicit initial ping during lookups
		// TODO: make this work now that we don't keep unverified entries in the main bucket
		if((!verifiedReachable() && failedQueries > 0) || failedQueries > 3)
			return false;
		return true;
	}
	
	public boolean verifiedReachable() {
		return verified;
	}
	
	public boolean neverContacted() {
		return lastSendTime == -1;
	}
	
	public int failedQueries() {
		return Math.abs(failedQueries);
	}
	
	public long lastSendTime() {
		return lastSendTime;
	}
	
	private boolean withinBackoffWindow(long now) {
		int backoff = PING_BACKOFF_BASE_INTERVAL << Math.min(MAX_TIMEOUTS, Math.max(0, failedQueries() - 1));
		
		return failedQueries != 0 && now - lastSendTime < backoff;
	}
	
	public long backoffWindowEnd() {
		if(failedQueries == 0 || lastSendTime <= 0)
			return -1L;
		
		int backoff = PING_BACKOFF_BASE_INTERVAL << Math.min(MAX_TIMEOUTS, Math.max(0, failedQueries() - 1));
		
		return lastSendTime + backoff;
	}
	
	public boolean withinBackoffWindow() {
		return withinBackoffWindow(System.currentTimeMillis());
	}
	
	public boolean needsPing() {
		long now = System.currentTimeMillis();

		if(withinBackoffWindow(now))
			return false;
		
		return failedQueries != 0 || now - lastSeen > OLD_AND_STALE_TIME;
	}

	// old entries, e.g. from routing table reload
	private boolean oldAndStale() {
		return failedQueries > OLD_AND_STALE_TIMEOUTS && System.currentTimeMillis() - lastSeen > OLD_AND_STALE_TIME;
	}
	
	public boolean removableWithoutReplacement() {
		// some non-reachable nodes may contact us repeatedly, bumping the last seen counter. they might be interesting to keep around so we can keep track of the backoff interval to not waste pings on them
		// but things we haven't heard from in a while can be discarded
		
		boolean seenSinceLastPing = lastSeen > lastSendTime;
		
		return failedQueries > MAX_TIMEOUTS && !seenSinceLastPing ;
	}
	
	public boolean needsReplacement() {
		return (failedQueries > 1 && !verifiedReachable()) || failedQueries > MAX_TIMEOUTS || oldAndStale();
	}

	public void mergeInTimestamps(KBucketEntry other) {
		if(!this.equals(other) || this == other)
			return;
		lastSeen = Math.max(lastSeen, other.getLastSeen());
		lastSendTime = Math.max(lastSendTime, other.lastSendTime);
		timeCreated = Math.min(timeCreated, other.getCreationTime());
		if(other.verifiedReachable())
			setVerified(true);
		if(!Double.isNaN(other.avgRTT.getAverage()) )
			avgRTT.updateAverage(other.avgRTT.getAverage());
	}
	
	public int getRTT() {
		return (int) avgRTT.getAverage(DHTConstants.RPC_CALL_TIMEOUT_MAX);
	}

	/**
	 * 
	 * @param rtt > 0 in ms. -1 if unknown
	 */
	public void signalResponse(long rtt) {
		lastSeen = System.currentTimeMillis();
		failedQueries = 0;
		verified = true;
		if(rtt > 0)
			avgRTT.updateAverage(rtt);
	}
	
	public void mergeRequestTime(long requestSent) {
		lastSendTime = Math.max(lastSendTime, requestSent);
	}
	
	public void signalScheduledRequest() {
		lastSendTime = System.currentTimeMillis();
	}


	/**
	 * Should be called to signal that a request to this peer has timed out;
	 */
	public void signalRequestTimeout () {
		failedQueries++;
	}
	
	
	void setVerified(boolean ver) {
		verified = ver;
		
	}
}
