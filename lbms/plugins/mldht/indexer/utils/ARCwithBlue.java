package lbms.plugins.mldht.indexer.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * an Adaptive Replacement Cache implementation using the Blue queue management algorithm for resizing to meet a throughput goal
 */
public class ARCwithBlue<K,V> {
	
	private final static Object THOMBSTONE = new Object();
	
	private final int baseCapacity;
	private final int maxCapacitz;
	private int currentCapacity;
	private int t1TargetSize;

	// ghost lists
	LinkedHashMap<K, Object> b1;
	LinkedHashMap<K, Object> b2;
	// live lists
	LinkedHashMap<K, V> t1;
	LinkedHashMap<K, V> t2;
	
	public ARCwithBlue(int baseSize, int maxSize) {
		this.baseCapacity = baseSize;
		this.maxCapacitz = maxSize;
		this.currentCapacity = baseSize;
		t1TargetSize = 0;
		
		t1 = new LinkedHashMap<K, V>(baseSize, 0.75f, true);
		b1 = new LinkedHashMap<K, Object>(baseSize, 0.75f, true);
		t2 = new LinkedHashMap<K, V>(baseSize, 0.75f, true);
		b2 = new LinkedHashMap<K, Object>(baseSize, 0.75f, true);
	}

	/**
	 * resizing the ARC to meet a throughput goal under the assumption that a queue overflow can be avoided with more caching
	 */
	public void adjust(BlueState goal)
	{
		// only resize if we've met or exceeded the goal of the previous resize
		int aggregateSize = t1.size()+t2.size()+b1.size()+b2.size();
		int oldCapacity = currentCapacity;
		
		if(goal == BlueState.QUEUE_EMPTY && currentCapacity > 2*baseCapacity && aggregateSize <= currentCapacity)
			currentCapacity -= baseCapacity;
		if(goal == BlueState.QUEUE_FULL && currentCapacity < maxCapacitz && aggregateSize >= currentCapacity)
			currentCapacity += baseCapacity;
		
		if(oldCapacity != currentCapacity)
			t1TargetSize = t1TargetSize*currentCapacity/oldCapacity;
	}
	
	public V get(K key)
	{
		return cache(key,null);
	}
	
	public void put(K key, V value)
	{
		cache(key,value);
	}
	
	
	/*
	 * ARC(c) I T1 = B1 = T2 = B2 = 0, p= 0. x - requested page.
	 */
	private V cache(K key, V value)
	{
		// Case I. x ∈ T1 ∪T2 (a hit in ARC(c) and DBL(2c)): Move x to the top of T2.
		if(t1.containsKey(key))
		{
			V found = t1.remove(key);
			t2.put(key, found);
			return found;
		}
		if(t2.containsKey(key))
		{ // touch for access order
			return t2.get(key);
		}
		
		// nullvalue == a get instead of an insert, don't perform the other ARC actions, they'll be done later by the real insert
		if(value == null)
			return null;
		
		/*
		 * Case II. x ∈ B1 (a miss in ARC(c), a hit in DBL(2c)):
		 * Adapt p= min{c,p+ max{|B2|/ |B1|, 1} } . REPLACE(p). Move x to the top of
		 * T2 and place it in the cache.  
		 */
		if(b1.containsKey(key))
		{
			t1TargetSize = Math.min(currentCapacity, t1TargetSize+Math.max(b2.size()/b1.size(), 1));
			replace(key);
			b1.remove(key);
			t2.put(key, value);
			return value;
		}
		/*
		 * Case III. x ∈ B2 (a miss in ARC(c), a hit in DBL(2c)):
		 * Adapt p= max{0,p - max{ |B1|/ |B2|, 1} } . REPLACE(p). Move x to the top
		 * of T2 and place it in the cache. 
		 */
		if(b2.containsKey(key))
		{
			t1TargetSize = Math.max(0, t1TargetSize-Math.max(b1.size()/b2.size(), 1));
			replace(key);
			b2.remove(key);
			t2.put(key, value);
			return value;
		}
		/* Case IV. x ∈ L1 ∪L2 (a miss in DBL(2c) and ARC(c)):        
		 * case ( i) |L1| = c:                                        
		 * 	If |T1| < c then delete the LRU page of B1 and REPLACE(p). 
		 * 	else delete LRU page of T1 and remove it from the cache.   
		 * 
		 * case ( ii) |L1| < c and |L1| + |L2| ≥ c:                   
		 * 	if |L1| + |L2| = 2c then delete the LRU page of B2.        
		 * 	REPLACE(p).                                                
		 * Put x at the top of T1 and place it in the cache.          
		 */
		int l1 = t1.size()+b1.size();
		int l2 = t2.size()+b2.size();
		
		
		// some slight modifications from the original algorithm to allow resizing
		if(l1 >= currentCapacity) 
		{
			if(b1.size() > 0)
			{
				pollEldest(b1);
				replace(key);
			} else
			{
				pollEldest(t1);
			}
		} else if(l1+l2 >= currentCapacity)
		{
			if(l1+l2 >= 2*currentCapacity)
				pollEldest(b2);
			replace(key);
		}
		
		// perform shrinking
		if(l1+l2 > 2*currentCapacity)
		{
			pollEldest(b1);
			pollEldest(b2);
		}
		
		t1.put(key, value);
		
		return value;
	}
	
	
	/**
		Subroutine REPLACE(p)
		if (|T1| ≥ 1) and ((x ∈ B2 and |T1| = p) or (|T1| > p)) then move the LRU page of
			T1 to the top of B1 and remove it from the cache.
		else move the LRU page in T2 to the top of B2 and remove it from the cache.
	 */
	private void replace(K key) {
		if(t1.size() >= 1 && ((b2.containsKey(key) && t1.size() == t1TargetSize) || t1.size() > t1TargetSize))
			b1.put(pollEldest(t1), THOMBSTONE);
		else
			b2.put(pollEldest(t2), THOMBSTONE);
	}
	
	private K pollEldest(LinkedHashMap<K, ?> m)
	{
		Iterator<K> it = m.keySet().iterator();
		if(!it.hasNext())
			return null;
		K k = it.next();
		it.remove();
		return k;
	}
	
}
