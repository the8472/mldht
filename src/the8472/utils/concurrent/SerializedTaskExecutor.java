package the8472.utils.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SerializedTaskExecutor {
	
	public static <T> Consumer<T> runSerialized(Consumer<T> task) {
		AtomicBoolean lock = new AtomicBoolean();
		Queue<T> q = new ConcurrentLinkedQueue<>();
		
		Predicate<T> tryRun = (T toTry) -> {
			boolean success = false;
			while(lock.compareAndSet(false, true)) {
				try {
					if(toTry != null) {
						task.accept(toTry);
						success = true;
					}
					T other;
					while((other = q.poll()) != null)
						task.accept(other);
				} finally {
					lock.set(false);
				}

				if(q.peek() == null)
					break;
			}
			return success;
		};
		
		return (T r) -> {
			
			// attempt to execute on current thread
			if(lock.get() == false && tryRun.test(r)) {
				// success
			} else {
				// execution on current thread failed, enqueue
				q.add(r);
				// try again in case other thread ceased draining the queue
				if(lock.get() == false)
					tryRun.test(null);
			}
		};
	}
	
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
