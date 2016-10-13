package lbms.plugins.mldht.kad;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class NonReachableCache {
	
	final static long PURGE_TIME_MULTIPLIER = TimeUnit.MINUTES.toMillis(5);
	// v6 addresses are unlikely to suffer from dynamic IP reuse, it should be safe to cache them longer
	final static long PURGE_TIME_MULTIPLIER_V6 = TimeUnit.MINUTES.toMillis(15);
	
	static class CacheEntry {
		long created;
		int failures;
	}
	
	ConcurrentHashMap<InetSocketAddress, CacheEntry> map = new ConcurrentHashMap<>();
	
	void onCallFinished(RPCCall c) {
		InetSocketAddress addr = c.getRequest().getDestination();
		RPCState state = c.state();
		
		if(state != RPCState.TIMEOUT && state != RPCState.RESPONDED)
			return;
			
		map.compute(addr, (k, oldEntry) -> {
			if(oldEntry != null) {
				CacheEntry updatedEntry = new CacheEntry();
				updatedEntry.created = oldEntry.created;
				// AIMD
				if(state == RPCState.TIMEOUT)
					updatedEntry.failures = oldEntry.failures + 1;
				else
					updatedEntry.failures /= 2;
				if(updatedEntry.failures == 0)
					return null;
					
				return updatedEntry;
			}
			
			CacheEntry newEntry = new CacheEntry();
			newEntry.created = System.currentTimeMillis();
			newEntry.failures = 1;
			
			return newEntry;
		});
	}
	
	public int getFailures(InetSocketAddress addr) {
		return Optional.ofNullable(map.get(addr)).map(e -> e.failures).orElse(0);
	}
	
	void cleanStaleEntries() {
		long now = System.currentTimeMillis();
		
		map.entrySet().removeIf(e -> {
			CacheEntry v = e.getValue();
			long multiplier = e.getKey().getAddress() instanceof Inet6Address ? PURGE_TIME_MULTIPLIER_V6 : PURGE_TIME_MULTIPLIER;
			return now - v.created > v.failures * multiplier;
		});
		
	}

}
