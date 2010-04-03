package lbms.plugins.mldht.kad;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.util.Comparator;

import org.gudy.azureus2.core3.util.TimeFormatter;

/**
 * Entry in a KBucket, it basically contains an ip_address of a node,
 * the udp port of the node and a node_id.
 *
 * @author Damokles
 */
public class KBucketEntry implements Serializable {

	/**
	 * ascending order for last seen, i.e. the last value will be the least recently seen one
	 */
	public static final Comparator<KBucketEntry> LAST_SEEN_ORDER = new Comparator<KBucketEntry>() {
		public int compare(KBucketEntry o1, KBucketEntry o2) {
			return Long.signum(o1.lastSeen - o2.lastSeen);
		}
	};

	/**
	 * ascending order for timeCreated, i.e. the first value will be the oldest
	 */
	public static final Comparator<KBucketEntry> AGE_ORDER = new Comparator<KBucketEntry>() {
		public int compare(KBucketEntry o1, KBucketEntry o2) {
			return Long.signum(o1.timeCreated - o2.timeCreated);
		}
	};

	
	public static final class DistanceOrder implements Comparator<KBucketEntry> {
		
		final Key target;
		public DistanceOrder(Key target) {
			this.target = target;
		}
	
		public int compare(KBucketEntry o1, KBucketEntry o2) {
			//return target.distance(o1.getID()).compareTo(target.distance(o2.getID()));
			return target.threeWayDistance(o1.getID(), o2.getID());
		}
	}

	private static final long	serialVersionUID	= 3230342110307814047L;

	private InetSocketAddress	addr;
	private Key					nodeID;
	private long				lastSeen;
	private int					failedQueries	= 0;
	private long				timeCreated;
	private String				version;

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
		timeCreated = timestamp;
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

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals (Object obj) {
		if (obj instanceof KBucketEntry) {
			KBucketEntry other = (KBucketEntry) obj;
			return nodeID.equals(other.nodeID)
					|| addr.getAddress().equals(other.addr.getAddress());
		}
		return super.equals(obj);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode () {
		return addr.hashCode() ^ nodeID.hashCode();
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
	
	public String toString() {
		long now = System.currentTimeMillis();
		return nodeID+"/"+addr+";seen:"+TimeFormatter.format(now-lastSeen)+";age:"+TimeFormatter.format(now-timeCreated)+(failedQueries>0?";fail:"+failedQueries:"");
	}

	/**
	 * Checks if the node is Good.
	 *
	 * A node is considered Good if it has responded in the last 15 min
	 *
	 * @return true if the node as responded in the last 15 min.
	 */
	public boolean isGood () {
		return !isQuestionable();
	}

	/**
	 * Checks if a node is Questionable.
	 *
	 * A node is considered Questionable if it hasn't responded in the last 15 min
	 *
	 * @return true if peer hasn't responded in the last 15 min.
	 */
	public boolean isQuestionable () {
		return (System.currentTimeMillis() - lastSeen > DHTConstants.KBE_QUESTIONABLE_TIME || isBad());
	}

	/**
	 * Checks if a node is Bad.
	 *
	 * A node is bad if it isn't good and hasn't responded the last 3 queries, or
	 * failed 5 times. i
	 *
	 * @return true if bad
	 */
	public boolean isBad () {
		if (failedQueries >= DHTConstants.KBE_BAD_IMMEDIATLY_ON_FAILED_QUERIES) {
	        return true;
        }
		if(System.currentTimeMillis() - lastSeen > DHTConstants.KBE_QUESTIONABLE_TIME &&
			failedQueries > DHTConstants.KBE_BAD_IF_FAILED_QUERIES_LARGER_THAN) {
	        return true;
        }
		return false;
	}

	/**
	 * Should be called to signal that the peer has sent a message to us, not necesarly a response to a query
	 */
	public void signalLastSeen() {
		lastSeen = System.currentTimeMillis();
	}
	
	public void mergeTimestamps (KBucketEntry entry) {
		if(!this.equals(entry))
			return;
		lastSeen = Math.max(lastSeen, entry.getLastSeen());
		timeCreated = Math.min(timeCreated, entry.getCreationTime());
	}

	/**
	 * Should be called to signal that the peer has responded
	 */
	public void signalResponse() {
		lastSeen = System.currentTimeMillis();
		failedQueries = 0;		
	}

	/**
	 * Should be called to signal that a request to this peer has timed out;
	 */
	public void signalRequestTimeout () {
		failedQueries++;
	}
}
