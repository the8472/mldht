package lbms.plugins.mldht.kad;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpamThrottle {
	
	private Map<InetAddress, Integer> hitcounter = new ConcurrentHashMap();
	
	private Instant lastDecayTime = Instant.now();
	
	private static final int BURST = 10;
	private static final int PER_SECOND = 2;
	
	public boolean addAndTest(InetAddress addr) {
		int updated = add(addr);
		
		if(updated >= BURST)
			return true;
		
		return false;
	}
	
	public void remove(InetAddress addr) {
		hitcounter.remove(addr);
	}
	
	public boolean test(InetAddress addr) {
		return hitcounter.getOrDefault(addr, 0) >= BURST;
	}
	
	public int add(InetAddress addr) {
		return hitcounter.compute(addr, (key, old) -> old == null ? 1 : Math.min(old + 1, BURST));
	}
	
	public void decay() {
		Instant now = Instant.now();
		long delta;
		synchronized (this) {
			delta = Duration.between(lastDecayTime, now).getSeconds();
			if(delta < 1)
				return;
			lastDecayTime = lastDecayTime.plusSeconds(delta);
		}
		
		hitcounter.replaceAll((k, v) -> (int) (v - delta * PER_SECOND));
		hitcounter.entrySet().removeIf(entry -> entry.getValue() <= 0);
	}
}
