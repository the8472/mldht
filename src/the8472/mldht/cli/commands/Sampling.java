package the8472.mldht.cli.commands;

import the8472.mldht.cli.CommandProcessor;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.tasks.KeyspaceSampler;
import lbms.plugins.mldht.kad.tasks.NodeLookup;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import lbms.plugins.mldht.kad.Prefix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Sampling extends CommandProcessor {
	
	NIOConnectionManager conMan;
	
	ScheduledThreadPoolExecutor timer;

	@Override
	protected void process() {
		
		AtomicInteger counter = new AtomicInteger();
		
		dhts.stream().filter(DHT::isRunning).forEach(dht -> {
			List<RPCServer> srvs =  dht.getServerManager().getAllServers().stream().filter(RPCServer::isReachable).collect(Collectors.toList());
			
			List<Prefix> pref = new ArrayList<>();
			pref.add(new Prefix());
			
			while(pref.size() < srvs.size()) {
				Prefix widest = pref.stream().min(Comparator.comparingInt(Prefix::getDepth)).get();
				pref.remove(widest);
				pref.add(widest.splitPrefixBranch(false));
				pref.add(widest.splitPrefixBranch(true));
			}
			
			Collections.sort(pref);
			
			srvs.forEach(srv -> {
				Prefix p = pref.remove(pref.size()-1);
				
				NodeLookup nl = new NodeLookup(p.first(), srv, dht.getNode(), false);
				nl.setInfo("seed lookup for keyspace sampler");
				
				counter.incrementAndGet();

				
				nl.addListener(unused -> {
					KeyspaceSampler t = new KeyspaceSampler(srv, dht.getNode(), p, nl, (c, k) -> {
						println(k.toString() + " src:" + c.getRequest().getDestination());
					});
					
					t.setInfo("sampling CLI call");

					t.addListener(unused2 -> {
						if(counter.decrementAndGet() == 0)
							exit(0);
					});

					dht.getTaskManager().addTask(t);
				});
				
				dht.getTaskManager().addTask(nl);
				
			});
			
			
		});

	}
	

}
