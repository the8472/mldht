package lbms.plugins.mldht.kad;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

public class SpamThrottle {
	
	private HashMap<InetAddress, Integer> hitcounter = new HashMap<>();
	
	private Instant lastDecayTime = Instant.now();
	
	private static final int BURST = 10;
	private static final int PER_SECOND = 2;
	
	public boolean isSpam(InetAddress addr) {
		decay();
		
		int updated = hitcounter.compute(addr, (key, old) -> old == null ? 1 : Math.min(old + 1, BURST));
		
		if(updated >= BURST)
			return true;
		
		return false;
	}
	
	public void decay() {
		Instant now = Instant.now();
		long delta = Duration.between(lastDecayTime, now).getSeconds();
		if(delta < 1)
			return;
		lastDecayTime = lastDecayTime.plusSeconds(delta);
		
		hitcounter.replaceAll((k, v) -> (int) (v - delta * PER_SECOND));
		hitcounter.entrySet().removeIf(entry -> entry.getValue() <= 0);
	}
}
