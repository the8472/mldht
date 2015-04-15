package the8472.utils.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class GuardedExclusiveTaskExecutor {
	
	public static Runnable whileTrue(BooleanSupplier whileCondition, Runnable loopBody) {
		AtomicBoolean lock = new AtomicBoolean();
		
		return () -> {
			// try acquire lock
			// if acquisition fails -> some other thread is stealing our work
			while(lock.compareAndSet(false, true)) {
				try {
					while(whileCondition.getAsBoolean())
						loopBody.run();
					
				} finally {
					// release lock
					// allow other thread to steal
					lock.set(false);
				}
				
				// but also try again in case there's more work to do and other threads trusted this one to finish the work
				if(!whileCondition.getAsBoolean())
					break;
			}
			
		};
	}

}
