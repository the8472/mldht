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

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

import lbms.plugins.mldht.kad.messages.MessageBase;

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
	
	private final RPCCallListener cl = new RPCCallListener() {
		public void onTimeout(RPCCall c) {
			Key nodeId = c.getExpectedID();
			if(nodeId == null)
				return;
			
			Entry<Key, CacheBucket> targetEntry = cache.floorEntry(nodeId);
			
			if(targetEntry == null || !targetEntry.getValue().prefix.isPrefixOf(nodeId))
				return;
			
			for(Iterator<KBucketEntry> it = targetEntry.getValue().entries.iterator();it.hasNext();)
			{
				KBucketEntry e = it.next();
				// remove an entry if the id matches
				// ignore the removal if we have heard from the node after the request has been issued, it might be a spurious failure
				if(e.getID().equals(nodeId) && (e.getLastSeen() < c.getSentTime() || c.getSentTime() == -1))
					it.remove();
			}

			
		}
		
		public void onStall(RPCCall c) {
			// TODO Auto-generated method stub
		}
		
		public void onResponse(RPCCall c, MessageBase rsp) {
			KBucketEntry kbe = new KBucketEntry(rsp.getOrigin(), rsp.getID());
			kbe.signalResponse(c.getRTT());
			add(kbe);
		}
	};
	
	public RPCCallListener getRPCListener() {
		return cl;
	}

	/*
	 * this insert procedure might cause minor inconsistencies (duplicate entries, too-large lists)
	 * but those are self-healing under merge/split operations
	 */
	public void add(KBucketEntry entryToInsert)
	{
		Key target = entryToInsert.getID();
		
		outer: while(true)
		{
			Entry<Key, CacheBucket> targetEntry = cache.floorEntry(target);
			
			if(targetEntry == null || !targetEntry.getValue().prefix.isPrefixOf(target))
			{ // split/merge operation ongoing, retry
				Thread.yield();
				continue;
			}
			
			CacheBucket targetBucket = targetEntry.getValue();
			
			int size = 0;
			
			for(Iterator<KBucketEntry> it = targetBucket.entries.iterator();it.hasNext();)
			{
				KBucketEntry e = it.next();
				size++;
				if(e.getID().equals(entryToInsert.getID()))
				{ // refresh timestamp, this is checked for removals
					e.mergeInTimestamps(entryToInsert);	
					break outer;
				}
			}
			
			if(size >= DHTConstants.MAX_CONCURRENT_REQUESTS)
			{
				// cache entry full, see if we this bucket prefix covers any anchor
				Map.Entry<Key, CacheAnchorPoint> anchorEntry = anchors.ceilingEntry(targetBucket.prefix);
											
				if(anchorEntry == null || !targetBucket.prefix.isPrefixOf(anchorEntry.getValue()))
				{
					for(Iterator<KBucketEntry> it=targetBucket.entries.iterator();it.hasNext();)
					{
						KBucketEntry kbe = it.next();
						if(entryToInsert.getRTT() < kbe.getRTT())
						{
							targetBucket.entries.add(entryToInsert);
							it.remove();
							break outer;
						}
					}
					break;
				}
					
				
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

					for(KBucketEntry e : targetBucket.entries)
						add(e);
				}

				continue;
			}
			
			targetBucket.entries.add(entryToInsert);
			break;
			
			
		}
	}

	
	public List<KBucketEntry> get(Key target, int targetSize)
	{
		ArrayList<KBucketEntry> closestSet = new ArrayList<KBucketEntry>(2 * targetSize);
		
		Map.Entry<Key, CacheBucket> ceil = cache.ceilingEntry(target);
		Map.Entry<Key, CacheBucket> floor = cache.floorEntry(target);
		
		do
		{
			if(floor != null)
			{
				closestSet.addAll(floor.getValue().entries);
				floor = cache.lowerEntry(floor.getKey());
			}
				
			if(ceil != null)
			{
				closestSet.addAll(ceil.getValue().entries);
				ceil = cache.higherEntry(ceil.getKey());
			}
		} while (closestSet.size() / 2 < targetSize && (floor != null || ceil != null));
		
		return closestSet;
	}
	
	public void cleanup(long now)
	{
		// first pass, eject old anchors
		for(Iterator<CacheAnchorPoint> it = anchors.values().iterator();it.hasNext();)
			if(now - it.next().expirationTime > 0)
				it.remove();
		
		Set seenEntries = new HashSet();
		
		// 2nd pass, eject old and/or duplicate entries
		for(Iterator<CacheBucket> it = cache.values().iterator();it.hasNext();)
		{
			CacheBucket b = it.next();
			
			for(Iterator<KBucketEntry> it2 = b.entries.iterator();it2.hasNext();)
			{
				KBucketEntry kbe = it2.next();
				if(seenEntries.contains(kbe.getID()) || seenEntries.contains(kbe.getAddress().getAddress()) || now - kbe.getLastSeen() > DHTConstants.ANNOUNCE_CACHE_MAX_AGE)
					it2.remove();
				seenEntries.add(kbe.getID());
				seenEntries.add(kbe.getAddress().getAddress());
			}

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
		StringBuilder bucketsBuilder = new StringBuilder();
		b.append("anchors ("+anchors.size()+"):\n");
		for(CacheAnchorPoint a : anchors.values())
			b.append(a).append('\n');
		
		int bucketCount = 0;
		int entryCount = 0;
		for(CacheBucket buck : cache.values())
		{
			int numEntries = buck.entries.size();
			bucketsBuilder.append(buck.prefix).append(" entries: ").append(numEntries).append('\n');
			bucketCount++;
			entryCount+= numEntries;
		}
		
		b.append("buckets ("+bucketCount+") / entries ("+entryCount+"):\n");
		b.append(bucketsBuilder);
		
		return b.toString();
	}
 	
	
}
