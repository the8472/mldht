package lbms.plugins.mldht.indexer.assemblyline;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.LogLevel;

public class AssemblyRunner implements Runnable {
	
	AssemblyTask t;
	ScheduledFuture<?> future;
	
	public AssemblyRunner(AssemblyTask t) {
		this.t = t;
	}
	
	public void submitToPool(ScheduledExecutorService pool,int delayInMillis)
	{
		future = pool.scheduleWithFixedDelay(this, 20000, delayInMillis , TimeUnit.MILLISECONDS);
	}
	
	public void cancel() {
		future.cancel(false);
	}
	
	public void run() {
		try {
			while(t.performTask())
				;
		} catch(Exception e) {
			DHT.log(e, LogLevel.Error);
		}
		
	}
}
