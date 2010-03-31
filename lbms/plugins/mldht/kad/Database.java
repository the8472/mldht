package lbms.plugins.mldht.kad;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.utils.ByteWrapper;

/**
 * @author Damokles
 * 
 */
public class Database {
	private Map<Key, Set<DBItem>>	items;
	private DatabaseStats			stats;
	private long timestampCurrent;
	private long timestampPrevious;
	
	private static byte[] sessionSecret = new byte[20];
	
	static {
		DHT.rand.nextBytes(sessionSecret);
	}

	Database()
	{
		stats = new DatabaseStats();
		items = new HashMap<Key, Set<DBItem>>();
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
		synchronized (items)
		{
			if (!items.containsKey(key))
			{
				insert(key);
			}
			Set<DBItem> keyEntries = items.get(key);
			if (!keyEntries.remove(dbi))
				stats.setItemCount(stats.getItemCount() + 1);
			keyEntries.add(dbi);
		}
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
		LinkedList<DBItem> tdbl = new LinkedList<DBItem>();
		synchronized (items)
		{
			if (!items.containsKey(key))
				return null;
			
			List<DBItem> dbl = new ArrayList<DBItem>(items.get(key));
			Collections.shuffle(dbl);
			for (DBItem item : dbl)
			{
				if (!(item instanceof PeerAddressDBItem))
					continue;
				PeerAddressDBItem it = (PeerAddressDBItem) item;
				if (it.getAddressType() != forType.PREFERRED_ADDRESS_TYPE)
					continue;
				if(preferPeers && it.isSeed())
					tdbl.addLast(it);
				else
					tdbl.addFirst(it);
			}
		}
		
		return tdbl.subList(0, Math.min(tdbl.size(), max_entries));		
	}
	
	BloomFilter createScrapeFilter(Key key, boolean seedFilter)
	{
		Set<DBItem> dbl = items.get(key);
		if (dbl == null || dbl.isEmpty())
			return null;
		
		BloomFilter filter = new BloomFilter();
		
		for (DBItem item : dbl)
		{
			if (!(item instanceof PeerAddressDBItem))
				continue;
			PeerAddressDBItem it = (PeerAddressDBItem) item;
			if(seedFilter == it.isSeed())
				filter.insert(it.getInetAddress());
		}
		
		return filter;
	}

	/**
	 * Expire all items older than 30 minutes
	 * 
	 * @param now
	 *            The time it is now (we pass this along so we only have to
	 *            calculate it once)
	 */
	void expire(long now) {
		int removed = 0;
		List<Key> keysToRemove = new ArrayList<Key>();
		synchronized (items)
		{
			int itemCount = 0;
			for (Key k : items.keySet())
			{
				Set<DBItem> dbl = items.get(k);
				List<DBItem> tmp = new ArrayList<DBItem>(dbl);
				Collections.sort(tmp, DBItem.ageOrdering);
				while (dbl.size() > 0 && tmp.get(0).expired(now))
				{
					dbl.remove(tmp.remove(0));
					removed++;
				}
				if (dbl.size() == 0)
				{
					keysToRemove.add(k);
				} else
					itemCount += dbl.size();
			}

			items.keySet().removeAll(keysToRemove);
			stats.setKeyCount(items.size());
			stats.setItemCount(itemCount);
		}
	}
	
	
	boolean insertForKeyAllowed(Key target)
	{
		synchronized (items)
		{
			Set<DBItem> entries = items.get(target);
			if(entries == null)
				return true;
			if(entries.size() < DHTConstants.MAX_DB_ENTRIES_PER_KEY)
				return true;
		}
		
		return false;
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
		bb.putLong(timestampCurrent);
		bb.put(lookupKey.getHash());
		bb.put(sessionSecret);
		try
		{
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(tdata);
			return new ByteWrapper(md.digest());

			
		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
			return new ByteWrapper(new byte[20]);
		}
	}
	
	private synchronized void updateTokenTimestamps() {
		if(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - timestampCurrent) > DHTConstants.TOKEN_TIMEOUT)
		{
			timestampPrevious = timestampCurrent;
			timestampCurrent = System.nanoTime();
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
		boolean valid = checkToken(token, ip, port, lookupKey, timestampCurrent) || checkToken(token, ip, port, lookupKey, timestampPrevious);
		if(!valid)
			DHT.logDebug("Received Invalid token from " + ip.getHostAddress());
		return valid;
	}


	private boolean checkToken(ByteWrapper token, InetAddress ip, int port, Key lookupKey, long timeStamp) {

		byte[] tdata = new byte[ip.getAddress().length + 2 + 8 + Key.SHA1_HASH_LENGTH + sessionSecret.length];
		ByteBuffer bb = ByteBuffer.wrap(tdata);
		bb.put(ip.getAddress());
		bb.putShort((short) port);
		bb.putLong(timeStamp);
		bb.put(lookupKey.getHash());
		bb.put(sessionSecret);
		
		try
		{
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(tdata);
			ByteWrapper ct = new ByteWrapper(md.digest());
			// compare the generated token to the one received
			return token.equals(ct);

		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
			return false;
		}
		
	}

	/// Test wether or not the DB contains a key
	boolean contains(Key key) {
		synchronized (items)
		{
			return items.containsKey(key);
		}
	}

	/// Insert an empty item (only if it isn't already in the DB)
	void insert(Key key) {
		synchronized (items)
		{
			Set<DBItem> dbl = items.get(key);
			if (dbl == null)
			{
				items.put(key, new HashSet<DBItem>());
				stats.setKeyCount(items.size());
			}
		}
	}

	/**
	 * @return the stats
	 */
	public DatabaseStats getStats() {
		return stats;
	}
}
