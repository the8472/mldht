package the8472.mldht.cli.commands;

import static the8472.bencode.Utils.buf2str;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.tasks.NodeLookup;
import the8472.mldht.cli.CommandProcessor;

public class Burst extends CommandProcessor {
	
	@Override
	protected void process() {
		Thread t = new Thread(() -> {
			int count = 50;
			
			if(arguments.size() > 0)
				count = Integer.parseInt(buf2str(ByteBuffer.wrap(arguments.get(0))));
			
			List<DHT> dhts = new ArrayList<>(this.dhts);
			
			List<CompletableFuture<Void>> futures = new ArrayList<>();
			
			for(int i=0;i<count;i++) {
				CompletableFuture<Void> future = new CompletableFuture<Void>();
				futures.add(future);
				DHT dht = dhts.get(ThreadLocalRandom.current().nextInt(dhts.size()));
				Optional.ofNullable(dht.getServerManager().getRandomActiveServer(false)).ifPresent(rpc -> {
					NodeLookup task = new NodeLookup(Key.createRandomKey(), rpc, dht.getNode(), false);
					task.addListener((finishedTask) -> {
						println("done: "+finishedTask);
						future.complete(null);
					});
					dht.getTaskManager().addTask(task);
				});
				
			}
			
			
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			
			println("all done");
			
			exit(0);
		});
		
		t.setDaemon(true);
		t.setName("burst command");
		
		t.start();
	}


}
