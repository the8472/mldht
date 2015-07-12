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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Comparator;

import lbms.plugins.mldht.utils.ExponentialWeightendMovingAverage;

/**
 * Entry in a KBucket, it basically contains an ip_address of a node,
 * the udp port of the node and a node_id.
 *
 * @author Damokles
 */
public class KBucketEntry implements Serializable {
	
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
	

	private static final long	serialVersionUID	= 3230342110307814047L;

	private InetSocketAddress	addr;
	private Key					nodeID;
	private long				lastSeen;
	
	/**
	 *   -1 = never queried / learned about it from incoming requests
	 *    0 = last query was a success
	 *  > 0 = query failed
	 */
	private int					failedQueries	= -1;
	private long				timeCreated;
	private String				version;
	private transient ExponentialWeightendMovingAverage avgRTT;
	private transient long lastSendTime;

	{ // delegate transient stuff to be handled the same way as on deserialization
		fieldInitializers();
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		fieldInitializers();
	}
	
	private void fieldInitializers() {
		avgRTT = new ExponentialWeightendMovingAverage().setWeight(RTT_EMA_WEIGHT);
		lastSendTime = -1;
	}

	/**
	 * Constructor, sets everything to 0.
	 * @return
	 */
	public KBucketEntry () {
		lastSeen = System.currentTimeMillis();
		timeCreated = lastSeen;
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
		return nodeID.equals(other.nodeID) && addr.getAddress().equals(other.addr.getAddress());
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
    public void setVersion (String version) {
	    this.version = version;
    }

	/**
     * @return the version
     */
    public String getVersion () {
	    return version;
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
		if(failedQueries < 0 || failedQueries > 1)
			return false;
		
		return true;
	}
	
	
	public boolean eligibleForLocalLookup() {
		// allow implicit initial ping during lookups
		if(failedQueries < -1 || failedQueries > 3)
			return false;
		return true;
	}
	
	public int failedQueries() {
		return Math.abs(failedQueries);
	}
	
	public boolean needsPing() {
		long now = System.currentTimeMillis();
		// backoff for pings
		if(now - lastSendTime < PING_BACKOFF_BASE_INTERVAL * failedQueries())
			return false;
			
		
		return failedQueries != 0 || now - lastSeen > OLD_AND_STALE_TIME;
	}

	// old entries, e.g. from routing table reload
	private boolean oldAndStale() {
		return failedQueries > OLD_AND_STALE_TIMEOUTS && System.currentTimeMillis() - lastSeen > OLD_AND_STALE_TIME;
	}
	
	public boolean needsReplacement() {
		return failedQueries < -1 || failedQueries > MAX_TIMEOUTS || oldAndStale();
	}

	public void mergeInTimestamps(KBucketEntry other) {
		if(!this.equals(other))
			return;
		lastSeen = Math.max(lastSeen, other.getLastSeen());
		lastSendTime = Math.max(lastSendTime, other.lastSendTime);
		timeCreated = Math.min(timeCreated, other.getCreationTime());
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
		if(rtt > 0)
			avgRTT.updateAverage(rtt);
	}
	
	public void signalScheduledRequest() {
		lastSendTime = System.currentTimeMillis();
	}


	/**
	 * Should be called to signal that a request to this peer has timed out;
	 */
	public void signalRequestTimeout () {
		if(failedQueries >= 0)
			failedQueries++;
		else
			failedQueries--;
	}
}
