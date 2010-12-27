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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class AnnounceNodeCache {
	
	private static class CacheAnchorPoint extends Key {
		public CacheAnchorPoint(Key k)
		{
			// reuse array to save memory
			hash = k.hash;
		}
		
		long expirationTime;
	}
	
	private static class CacheBucket {
	
		Prefix prefix;
		ConcurrentLinkedQueue<KBucketEntry> entries = new ConcurrentLinkedQueue<KBucketEntry>();
		
		public CacheBucket(Prefix p) {
			prefix = p;
		}
	}
	
	ConcurrentSkipListMap<Key, CacheAnchorPoint> anchors = new ConcurrentSkipListMap<Key,CacheAnchorPoint>();
	ConcurrentSkipListMap<Key, CacheBucket> cache = new ConcurrentSkipListMap<Key, AnnounceNodeCache.CacheBucket>();
	
	
	public AnnounceNodeCache() {
		CacheBucket rootBucket = new CacheBucket(new Prefix());
		cache.put(rootBucket.prefix, rootBucket);
	}
	
	public void register(Key target, boolean isFastLookup)
	{
		CacheAnchorPoint anchor = new CacheAnchorPoint(target);
		anchor.expirationTime = System.currentTimeMillis() + (isFastLookup ? DHTConstants.ANNOUNCE_CACHE_FAST_LOOKUP_AGE : DHTConstants.ANNOUNCE_CACHE_MAX_AGE); 
		anchors.put(target,anchor);
	}
	
	public void removeEntry(Key nodeId)
	{
		if(nodeId != null)
		{
			Entry<Key, CacheBucket> targetEntry = cache.floorEntry(nodeId);
			
			if(targetEntry == null || !targetEntry.getValue().prefix.isPrefixOf(nodeId))
				return;
			
			for(Iterator<KBucketEntry> it = targetEntry.getValue().entries.iterator();it.hasNext();)
				if(it.next().getID().equals(nodeId))
					it.remove();
		}
			
	}
	
	public void add(KBucketEntry entryToInsert)
	{
		Key target = entryToInsert.getID();
		
		while(true)
		{
			Entry<Key, CacheBucket> targetEntry = cache.floorEntry(target);
			
			if(targetEntry == null || !targetEntry.getValue().prefix.isPrefixOf(target))
			{ // split/merge operation ongoing, retry
				Thread.yield();
				continue;
			}
			
			CacheBucket targetBucket = targetEntry.getValue();
			
			if(targetBucket.entries.contains(entryToInsert))
				break;
			
			if(targetBucket.entries.size() >= DHTConstants.MAX_CONCURRENT_REQUESTS)
			{
				// cache entry full, see if we this bucket prefix covers any anchor
				Map.Entry<Key, CacheAnchorPoint> anchorEntry = anchors.ceilingEntry(targetBucket.prefix);
											
				if(anchorEntry == null || !targetBucket.prefix.isPrefixOf(anchorEntry.getValue()))
					break;
				
				synchronized (targetBucket)
				{
					// check for concurrent split/merge
					if(cache.get(targetBucket.prefix) != targetBucket)
						continue;
					// perform split operation
					CacheBucket lowerBucket = new CacheBucket(targetBucket.prefix.splitPrefixBranch(false));
					CacheBucket upperBucket = new CacheBucket(targetBucket.prefix.splitPrefixBranch(true));
					
					// remove old entry. this leads to a temporary gap in the cache-keyspace!
					if(!cache.remove(targetEntry.getKey(),targetBucket))
						continue;
					
					
					cache.put(upperBucket.prefix, upperBucket);
					cache.put(lowerBucket.prefix, lowerBucket);
					
					// refill asap
					for(KBucketEntry e : targetBucket.entries)
						add(e);
				}
				
				continue;
			}
			
			targetBucket.entries.add(entryToInsert);
			break;
			
			
		}
	}

	
	public List<KBucketEntry> get(Key target, int count, Set<KBucketEntry> toExclude)
	{
		ArrayList<KBucketEntry> closestSet = new ArrayList<KBucketEntry>(2 * count);
		
		Map.Entry<Key, CacheBucket> ceil = cache.ceilingEntry(target);
		Map.Entry<Key, CacheBucket> floor = cache.floorEntry(target);
		if(ceil != null)
			closestSet.addAll(ceil.getValue().entries);
		if(floor != null && (ceil == null || floor.getValue() != ceil.getValue()))
			closestSet.addAll(floor.getValue().entries);
		
		// note that an expanding binary search only yields an _approximate_ closest set since we're not using buckets
		while(closestSet.size() / 2 < count && (floor != null || ceil != null))
		{
			if(floor != null)
				floor = cache.lowerEntry(floor.getKey());
			if(ceil != null)
				ceil = cache.higherEntry(ceil.getKey());

			if(ceil != null)
				closestSet.addAll(ceil.getValue().entries);
				
			if(floor != null)
				closestSet.addAll(floor.getValue().entries);
		}
		
		closestSet.removeAll(toExclude);
		
		Collections.sort(closestSet, new KBucketEntry.DistanceOrder(target));
		
		for(int i=closestSet.size()-1;i>=count;i--)
			closestSet.remove(i);
		
		return closestSet;
	}
	
	public void cleanup(long now)
	{
		// first pass, eject old anchors
		for(Iterator<CacheAnchorPoint> it = anchors.values().iterator();it.hasNext();)
			if(now - it.next().expirationTime > 0)
				it.remove();
		
		// 2nd pass, eject old entries
		for(Iterator<CacheBucket> it = cache.values().iterator();it.hasNext();)
		{
			CacheBucket b = it.next();
			for(Iterator<KBucketEntry> it2 = b.entries.iterator();it2.hasNext();)
				if(now - it2.next().getLastSeen() > DHTConstants.ANNOUNCE_CACHE_MAX_AGE)
					it2.remove();
		}

		
		// merge buckets that aren't full or don't have anchors
		
		Entry<Key,CacheBucket> entry = cache.firstEntry();
		if(entry == null)
			return;
		
		CacheBucket current = null;
		CacheBucket next = entry.getValue();

		while(true)
		{
			current = next;
			
			entry = cache.higherEntry(current.prefix);
			if(entry == null)
				return;
			next = entry.getValue();
			
			
			if(!current.prefix.isSiblingOf(next.prefix))
				continue;
			
			
			Prefix parent = current.prefix.getParentPrefix();
			Map.Entry<Key, CacheAnchorPoint> anchor = anchors.ceilingEntry(parent);
			if(anchor == null || !parent.isPrefixOf(anchor.getValue()) || current.entries.size()+next.entries.size() < DHTConstants.MAX_CONCURRENT_REQUESTS)
			{
				synchronized (current)
				{
					synchronized (next)
					{
						// check for concurrent split/merge
						if(cache.get(current.prefix) != current || cache.get(next.prefix) != next)
							continue;
						
						cache.remove(current.prefix, current);
						cache.remove(next.prefix,next);
						
						cache.put(parent, new CacheBucket(parent));
						
						for(KBucketEntry e : current.entries)
							add(e);
						for(KBucketEntry e : next.entries)
							add(e);
					}
				}
				
				// move backwards if possible to cascade merges backwards if necessary
				entry = cache.lowerEntry(current.prefix);
				if(entry == null)
					continue;
				next = entry.getValue();
			}
		}
					
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("anchors:\n");
		for(CacheAnchorPoint a : anchors.values())
			b.append(a).append('\n');
		b.append("buckets:\n");
		for(CacheBucket buck : cache.values())
			b.append(buck.prefix).append(" entries: ").append(buck.entries.size()).append('\n');
		
		return b.toString();
	}
 	
	
}
