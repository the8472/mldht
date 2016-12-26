package the8472.mldht.cli.commands;

import the8472.mldht.cli.CommandProcessor;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.tasks.KeyspaceSampler;
import lbms.plugins.mldht.utils.NIOConnectionManager;

import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class Sampling extends CommandProcessor {
	
	NIOConnectionManager conMan;
	
	ScheduledThreadPoolExecutor timer;

	@Override
	protected void process() {
		
		AtomicInteger counter = new AtomicInteger();
		
		dhts.stream().filter(DHT::isRunning).map(DHT::getServerManager).map(m -> m.getRandomActiveServer(false)).filter(Objects::nonNull).forEach(d -> {
			DHT dht = d.getDHT();

			KeyspaceSampler t = new KeyspaceSampler(d, dht.getNode(), (c, k) -> {
				println(k.toString() + " src:" + c.getRequest().getDestination());
			});

			counter.incrementAndGet();

			t.addListener(unused -> {
				if(counter.decrementAndGet() == 0)
					exit(0);
			});

			dht.getTaskManager().addTask(t);
		});



	}
	

}
