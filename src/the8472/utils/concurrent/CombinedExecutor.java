package the8472.utils.concurrent;

import static the8472.utils.Functional.tap;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wraps two pools, one for immediate execution of async, fast tasks. One for scheduled, potentially long-running tasks
 * reduces lock contention from the scheduler queue and interference from long-running tasks
 */
public class CombinedExecutor implements ScheduledExecutorService {
	
	ThreadPoolExecutor immediateExecutor;
	ScheduledThreadPoolExecutor scheduledExecutor;
	ThreadGroup group;
	
	public CombinedExecutor(String name, UncaughtExceptionHandler handler) {
		
		group = new ThreadGroup("name");
		
		group.setDaemon(true);
		
		Function<Runnable, Thread> f = (r) -> {
			Thread t = new Thread(group, r);
			if(handler != null)
				t.setUncaughtExceptionHandler(handler);
			t.setDaemon(true);
			return t;
		};
		
		
		immediateExecutor = new ThreadPoolExecutor(3, Runtime.getRuntime().availableProcessors(), 4, TimeUnit.SECONDS, new LinkedTransferQueue<Runnable>());
		immediateExecutor.setThreadFactory(f.andThen(t -> tap(t, t2  -> t2.setName(name + " immediate")))::apply);
		scheduledExecutor = new ScheduledThreadPoolExecutor(3);
		scheduledExecutor.setThreadFactory(f.andThen(t -> tap(t, t2  -> t2.setName(name + " scheduled")))::apply);
	}
	

	@Override
	public void shutdown() {
		immediateExecutor.shutdown();
		scheduledExecutor.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return tap(new ArrayList<>(immediateExecutor.shutdownNow()), l -> l.addAll(scheduledExecutor.shutdownNow()));
	}

	@Override
	public boolean isShutdown() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isTerminated() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return immediateExecutor.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return immediateExecutor.submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return immediateExecutor.submit(task);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return immediateExecutor.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		return scheduledExecutor.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return immediateExecutor.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return scheduledExecutor.invokeAny(tasks, timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		immediateExecutor.execute(command);
	}
	
	private static class SchedF extends FutureTask<Void> implements RunnableScheduledFuture<Void> {
		
		static AtomicInteger seqNumGen = new AtomicInteger();
		
		int seqNum;
		long nanos;
		
		public SchedF(Runnable r, long delay, TimeUnit u) {
			super(r,null);
			seqNum = seqNumGen.incrementAndGet();
			this.nanos =  System.nanoTime() + TimeUnit.NANOSECONDS.convert(delay, u);
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(nanos - System.nanoTime(), TimeUnit.NANOSECONDS);
		}

		@Override
		public int compareTo(Delayed o) {
			long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
			
			if(diff == 0 & o != this) {
				if(o instanceof SchedF)
					diff = this.seqNum - ((SchedF) o).seqNum;
				else
					diff = -1;
			}
			
			return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
		}

		@Override
		public boolean isPeriodic() {
			return false;
		}
		
	}

	final Consumer<RunnableScheduledFuture<Void>> serialSchedule = SerializedTaskExecutor.runSerialized((f) -> {
		scheduledExecutor.getQueue().offer(f);
	});

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		// roll our own to avoid thread suspension due to queue locking when hammering the scheduler with lots of one-off submissions
		RunnableScheduledFuture<Void> future = new SchedF(command, delay, unit);

		serialSchedule.accept(future);
		
		return future;
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return scheduledExecutor.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return scheduledExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return scheduledExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

}
