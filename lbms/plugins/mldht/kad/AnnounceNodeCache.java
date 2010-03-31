package lbms.plugins.mldht.kad;

import java.util.*;

public class AnnounceNodeCache {
	
	private class CacheEntry {		
		public CacheEntry(Collection<KBucketEntry> entries) {
			this.entries = entries;
		}
		
		long now = System.currentTimeMillis();
		Collection<KBucketEntry> entries;
	}
	
	Map<Key, CacheEntry> cache = new HashMap<Key, CacheEntry>();
	
	public void add(Key target, Collection<KBucketEntry> nodes)
	{
		cache.put(target, new CacheEntry(nodes));
	}
	
	public Collection<KBucketEntry> get(Key target)
	{
		CacheEntry entry = cache.get(target);
		return entry == null ? Collections.EMPTY_LIST : entry.entries;
	}
	
	public void cleanup(long now)
	{
		for(Iterator<Map.Entry<Key, CacheEntry>> it = cache.entrySet().iterator();it.hasNext();)
			if(now - it.next().getValue().now > DHTConstants.ANNOUNCE_CACHE_MAX_AGE)
				it.remove();
	}
 	
	
}
