package the8472.mldht.cli.commands;

import static the8472.bencode.Utils.buf2str;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Formatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import the8472.mldht.cli.CommandProcessor;

public class GetPeers extends CommandProcessor {
	
	NIOConnectionManager conMan;
	
	ScheduledThreadPoolExecutor timer;

	@Override
	protected void process() {
		
		List<Key> hashes = arguments.stream()
				.map(bytes -> buf2str(ByteBuffer.wrap(bytes)))
				.filter(Key.STRING_PATTERN.asPredicate())
				.map(st -> new Key(st))
				.collect(Collectors.toList());
		
		AtomicInteger counter = new AtomicInteger();
		Instant start = Instant.now();
		
		hashes.forEach(h -> {
			dhts.stream().filter(DHT::isRunning).map(DHT::getServerManager).map(m -> m.getRandomActiveServer(false)).filter(Objects::nonNull).forEach(d -> {
				DHT dht = d.getDHT();
				
				PeerLookupTask t = new PeerLookupTask(d, dht.getNode(), h);
				
				counter.incrementAndGet();
				
				t.addListener(unused -> {
					if(counter.decrementAndGet() == 0)
						exit(0);
				});
				
				t.setResultHandler(item -> {
					Formatter f = new Formatter();
					
					Duration elapsed = Duration.between(start, Instant.now());
					
					f.format("%-5dms %s %s", elapsed.toMillis(), h.toString(), item.toString());
					
					println(f.toString());
				});
				
				dht.getTaskManager().addTask(t);
			});
		});
		
		
		
	}
	

}
