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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.utils.ByteWrapper;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

/**
 * @author Damokles
 * 
 */
public class Database {
	private ConcurrentMap<Key, ItemSet>	items;
	private DatabaseStats			stats;
	private AtomicLong timestampCurrent = new AtomicLong();
	private volatile long timestampPrevious;
	
	private static byte[] sessionSecret = new byte[20];
	
	static {
		ThreadLocalUtils.getThreadLocalRandom().nextBytes(sessionSecret);
	}

	Database()
	{
		stats = new DatabaseStats();
		items = new ConcurrentHashMap<Key, ItemSet>(3000);
	}
	
	private static class ItemSet {
		static final DBItem[] NO_ITEMS = new DBItem[0];
		
		private volatile DBItem[] items = NO_ITEMS;
		private volatile BloomFilterBEP33 seedFilter = null;
		private volatile BloomFilterBEP33 peerFilter = null;
		
		public ItemSet(DBItem... initial) {
			items = initial;
		}
		
		/**
		 * @return true when inserting, false when replacing
		 */
		private boolean add(DBItem toAdd) {
			synchronized (this) {
				DBItem[] current = items;
				int idx = Arrays.asList(current).indexOf(toAdd);
				if(idx >= 0) {
					current[idx] = toAdd;
					return false;
				}
				
				DBItem[] newItems = Arrays.copyOf(current, current.length + 1);
				newItems[newItems.length-1] = toAdd;
				Collections.shuffle(Arrays.asList(newItems));

				items = newItems;
				return true;
												
			}
		}
		
		DBItem[] snapshot() {
			return items;
		}
		
		boolean isEmpty() {
			return items.length == 0;
		}
		
		int size() {
			return items.length;
		}
		
		private void modified() {
			peerFilter = null;
			seedFilter = null;
		}
		
		public BloomFilterBEP33 getSeedFilter() {
			BloomFilterBEP33 f = seedFilter;
			if(f == null) {
				f = buildFilter(true);
				seedFilter = f;
			}
			
			return f;
		}
		
		public BloomFilterBEP33 getPeerFilter() {
			BloomFilterBEP33 f = peerFilter;
			if(f == null) {
				f = buildFilter(false);
				peerFilter = f;
			}
			
			return f;
		}
		
		private BloomFilterBEP33 buildFilter(boolean seed) {
			if(items.length == 0)
				return null;
			
			BloomFilterBEP33 filter = new BloomFilterBEP33();

			for (DBItem item : items)
			{
				if (!(item instanceof PeerAddressDBItem))
					continue;
				PeerAddressDBItem it = (PeerAddressDBItem) item;
				if (seed == it.isSeed())
					filter.insert(it.getInetAddress());
			}
			
			return filter;
		}
		
		void expire() {
			synchronized (this) {
				long now = System.currentTimeMillis();
				
				DBItem[] items = this.items;
				DBItem[] newItems = new DBItem[items.length];
				
				// don't remove all at once -> smears out new registrations on popular keys over time
				int toRemove = DHTConstants.MAX_DB_ENTRIES_PER_KEY / 5;
				
				int insertPoint = 0;
				
				for(int i=0;i<items.length;i++) {
					DBItem e = items[i];
					if(toRemove == 0 || !e.expired(now))
						newItems[insertPoint++] = e;
					else
						toRemove--;
				}
				
				if(insertPoint != newItems.length) {
					this.items = Arrays.copyOf(newItems, insertPoint);
					modified();
				}
				
			}
			
		}
	}

	/**
	 * Store an entry in the database
	 * 
	 * @param key
	 *            The key
	 * @param dbi
	 *            The DBItem to store
	 */
	public void store(Key key, DBItem dbi) {
		
		
		ItemSet keyEntries = null;
		
		ItemSet insertCanidate = new ItemSet(dbi);
		
		keyEntries = items.putIfAbsent(key, insertCanidate);
		
		if(keyEntries == null)
		{ // this only happens when inserting new keys... the load of .size should be bearable
			keyEntries = insertCanidate;
			stats.setKeyCount(items.size());
		}
		
		if(keyEntries.add(dbi))
			stats.setItemCount(stats.getItemCount() + 1);
	}

	/**
	 * Get max_entries items from the database, which have the same key, items
	 * are taken randomly from the list. If the key is not present no items will
	 * be returned, if there are fewer then max_entries items for the key, all
	 * entries will be returned
	 * 
	 * @param key
	 *            The key to search for
	 * @param dbl
	 *            The list to store the items in
	 * @param max_entries
	 *            The maximum number entries
	 */
	List<DBItem> sample(Key key, int max_entries, DHTtype forType, boolean preferPeers) {
		ItemSet keyEntry = null;
		DBItem[] snapshot = null;


		keyEntry = items.get(key);
		if(keyEntry == null)
			return null;
		
		snapshot = keyEntry.snapshot();
		
		if(snapshot.length == 0)
			return null;
		
		List<DBItem> peerlist = new ArrayList<DBItem>(Math.min(max_entries, snapshot.length));
		
		int offset = ThreadLocalRandom.current().nextInt(snapshot.length);
		
		preferPeers &= snapshot.length > max_entries;
		
		// try to fill with peer items if so requested
		for(int i=0;i<snapshot.length;i++) {
			PeerAddressDBItem item = (PeerAddressDBItem) snapshot[(i + offset)%snapshot.length];
			assert(item.getAddressType() == forType.PREFERRED_ADDRESS_TYPE);
			if(!item.isSeed() || !preferPeers)
				peerlist.add(item);
			if(peerlist.size() == max_entries)
				break;
		}
		
		// failed to find enough peer items, try seeds
		if(preferPeers && peerlist.size() < max_entries) {
			for(int i=0;i<snapshot.length;i++) {
				PeerAddressDBItem item = (PeerAddressDBItem) snapshot[(i + offset)%snapshot.length];
				assert(item.getAddressType() == forType.PREFERRED_ADDRESS_TYPE);
				if(item.isSeed())
					peerlist.add(item);
				if(peerlist.size() == max_entries)
					break;
			}
		}

		return peerlist;
	}
	
	BloomFilterBEP33 createScrapeFilter(Key key, boolean seedFilter)
	{
		ItemSet dbl = items.get(key);
		
		if (dbl == null)
			return null;
		
		return seedFilter ? dbl.getSeedFilter() : dbl.getPeerFilter();
	}

	/**
	 * Expire all items older than 30 minutes
	 * 
	 * @param now
	 *            The time it is now (we pass this along so we only have to
	 *            calculate it once)
	 */
	void expire(long now) {
		
		int itemCount = 0;
		for (ItemSet dbl : items.values())
		{
			dbl.expire();
			itemCount += dbl.size();
		}
		
		items.entrySet().removeIf(e -> e.getValue().isEmpty());
		

		stats.setKeyCount(items.size());
		stats.setItemCount(itemCount);
	}
	
	
	boolean insertForKeyAllowed(Key target)
	{
		ItemSet entries = items.get(target);
		if(entries == null)
			return true;
		
		int size = entries.size();
		
		if(size >= DHTConstants.MAX_DB_ENTRIES_PER_KEY)
			return false;
		
		if(size < DHTConstants.MAX_DB_ENTRIES_PER_KEY / 5)
			return true;
		
		// implement RED to throttle write attempts
		return size < ThreadLocalRandom.current().nextInt(DHTConstants.MAX_DB_ENTRIES_PER_KEY);
	}
	
	/**
	 * Generate a write token, which will give peers write access to the DB.
	 * 
	 * @param ip
	 *            The IP of the peer
	 * @param port
	 *            The port of the peer
	 * @return A Key
	 */
	ByteWrapper genToken(InetAddress ip, int port, Key lookupKey) {
		updateTokenTimestamps();
		
		byte[] tdata = new byte[ip.getAddress().length + 2 + 8 + Key.SHA1_HASH_LENGTH + sessionSecret.length];
		// generate a hash of the ip port and the current time
		// should prevent anybody from crapping things up
		ByteBuffer bb = ByteBuffer.wrap(tdata);
		bb.put(ip.getAddress());
		bb.putShort((short) port);
		bb.putLong(timestampCurrent.get());
		bb.put(lookupKey.getHash());
		bb.put(sessionSecret);
		
		// shorten 4bytes to not waste packet size
		// the chance of guessing correctly would be 1 : 4 million and only be valid for a single infohash
		byte[] token = Arrays.copyOf(ThreadLocalUtils.getThreadLocalSHA1().digest(tdata), 4);
		
		
		return new ByteWrapper(token);
	}
	
	private void updateTokenTimestamps() {
		long current = timestampCurrent.get();
		long now = System.nanoTime();
		while(TimeUnit.NANOSECONDS.toMillis(now - current) > DHTConstants.TOKEN_TIMEOUT)
		{
			if(timestampCurrent.compareAndSet(current, now))
			{
				timestampPrevious = current;
				break;
			}
			current = timestampCurrent.get();
		}
	}

	/**
	 * Check if a received token is OK.
	 * 
	 * @param token
	 *            The token received
	 * @param ip
	 *            The ip of the sender
	 * @param port
	 *            The port of the sender
	 * @return true if the token was given to this peer, false other wise
	 */
	boolean checkToken(ByteWrapper token, InetAddress ip, int port, Key lookupKey) {
		updateTokenTimestamps();
		boolean valid = checkToken(token, ip, port, lookupKey, timestampCurrent.get()) || checkToken(token, ip, port, lookupKey, timestampPrevious);
		if(!valid)
			DHT.logDebug("Received Invalid token from " + ip.getHostAddress());
		return valid;
	}


	private boolean checkToken(ByteWrapper toCheck, InetAddress ip, int port, Key lookupKey, long timeStamp) {

		byte[] tdata = new byte[ip.getAddress().length + 2 + 8 + Key.SHA1_HASH_LENGTH + sessionSecret.length];
		ByteBuffer bb = ByteBuffer.wrap(tdata);
		bb.put(ip.getAddress());
		bb.putShort((short) port);
		bb.putLong(timeStamp);
		bb.put(lookupKey.getHash());
		bb.put(sessionSecret);
		
		byte[] rawToken = Arrays.copyOf(ThreadLocalUtils.getThreadLocalSHA1().digest(tdata), 4);
		
		return toCheck.equals(new ByteWrapper(rawToken));
	}
	
	public Map<Key, List<DBItem>> getData() {
		return items.entrySet().stream().collect(Collectors.toMap((e) -> {
			return e.getKey();
		}, (e) -> {
			return Arrays.asList(e.getValue().snapshot());
		}));
	}


	/**
	 * @return the stats
	 */
	public DatabaseStats getStats() {
		return stats;
	}
}
