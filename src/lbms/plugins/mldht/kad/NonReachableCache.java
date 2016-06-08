package lbms.plugins.mldht.kad;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class NonReachableCache {
	
	final static long PURGE_TIME_MULTIPLIER = TimeUnit.MINUTES.toMillis(5);
	
	static class CacheEntry {
		long created;
		int failures;
	}
	
	ConcurrentHashMap<InetSocketAddress, CacheEntry> map = new ConcurrentHashMap<>();
	
	void onCallFinished(RPCCall c) {
		InetSocketAddress addr = c.getRequest().getDestination();
		RPCState state = c.state();
		if(state == RPCState.RESPONDED) {
			map.remove(addr);
			return;
		}
		
		if(state != RPCState.TIMEOUT)
			return;
			
		map.compute(addr, (k, oldEntry) -> {
			if(oldEntry != null) {
				CacheEntry updatedEntry = new CacheEntry();
				updatedEntry.created = oldEntry.created;
				updatedEntry.failures = oldEntry.failures + 1;
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
		
		map.values().removeIf(v -> {
			return now - v.created > v.failures * PURGE_TIME_MULTIPLIER;
		});
		
	}

}
