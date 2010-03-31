package lbms.plugins.mldht.kad;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.utils.PackUtil;

/**
 * @author Damokles
 *
 */
public class KClosestNodesSearch {
	private Key								targetKey;
	private SortedMap<Key, KBucketEntry>	entries = new TreeMap<Key, KBucketEntry>();
	private int								max_entries;
	private DHT								owner;

	/**
	 * Constructor sets the key to compare with
	 * @param key The key to compare with
	 * @param max_entries The maximum number of entries can be in the map
	 * @return
	 */
	public KClosestNodesSearch (Key key, int max_entries, DHT owner) {
		this.targetKey = key;
		this.owner = owner;
		this.max_entries = max_entries;
	}

	/**
	 * @return the Target key of the search
	 */
	public Key getSearchTarget () {
		return targetKey;
	}

	/**
	 * @return the number of entries
	 */
	public int getNumEntries () {
		return entries.size();
	}

	/**
	 * Try to insert an entry.
	 * @param e The entry
	 * @return true if insert was successful
	 */
	public boolean tryInsert (KBucketEntry e) {
		// calculate distance between key and e
		Key dist = targetKey.distance(e.getID());

		if (entries.size() < max_entries) {
			// room in the map so just insert
			entries.put(dist, e);
			return true;
		}

		// now find the max distance
		// seeing that the last element of the map has also
		// the biggest distance to key (std::map is sorted on the distance)
		// we just take the last
		Key max = entries.lastKey();
		if (dist.compareTo(max) == -1)
		{
			// insert if d is smaller then max
			entries.put(dist, e);
			// erase the old max value
			entries.remove(max);
			return true;
		}
		return false;
		
	}
	
	public void fill()
	{
		fill(false);
	}
	
	public void fill(boolean includeOurself) {
		owner.getNode().findKClosestNodes(this);
		
		if(includeOurself && owner.getServer().getPublicAddress() != null && entries.size() < max_entries)
		{
			InetSocketAddress sockAddr = new InetSocketAddress(owner.getServer().getPublicAddress(), owner.getServer().getPort());
			entries.put(targetKey.distance(owner.getOurID()), new KBucketEntry(sockAddr, owner.getOurID()));
		}
	}

	public boolean isFull () {
		return entries.size() >= max_entries;
	}

	
	/**
	 * Packs the results in a byte array.
	 *
	 * @return the encoded results.
	 */
	public byte[] pack () {
		if(entries.size() == 0)
			return null;
		int entryLength = owner.getType().NODES_ENTRY_LENGTH;
		
		byte[] buffer = new byte[entries.size() * entryLength];
		int max_items = buffer.length / 26;
		int j = 0;

		for (KBucketEntry e : entries.values()) {
			if (j >= max_items) {
				break;
			}
			PackUtil.PackBucketEntry(e, buffer, j * entryLength, owner.getType());
			j++;
		}
		return buffer;
	}

	/**
	 * @return a unmodifiable Collection of the entries
	 */
	public Collection<KBucketEntry> getEntries () {
		return Collections.unmodifiableCollection(entries.values());
	}
}
