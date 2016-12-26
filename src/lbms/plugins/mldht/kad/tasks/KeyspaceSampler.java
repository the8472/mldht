package lbms.plugins.mldht.kad.tasks;

import lbms.plugins.mldht.indexer.utils.GenericBloomFilter;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.Prefix;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.SampleRequest;
import lbms.plugins.mldht.kad.messages.SampleResponse;
import lbms.plugins.mldht.kad.utils.AddressUtils;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

public class KeyspaceSampler extends Task {
	
	Set<KBucketEntry> candidates = new ConcurrentHashMap<KBucketEntry, Boolean>().keySet(Boolean.TRUE);
	GenericBloomFilter visited;
	BiConsumer<RPCCall, Key> ihcallback;
	int maxDepth;
	

	public KeyspaceSampler(RPCServer rpc, Node node, BiConsumer<RPCCall,Key> callback) {
		super(rpc, node);
		this.ihcallback = callback;
	}

	@Override
	void update() {
		while(canDoRequest() && !isDone()) {
			candidates.stream().findAny().ifPresent(kbe -> {

				if(visited.probablyContains(ByteBuffer.wrap(kbe.getAddress().getAddress().getAddress()))) {
					candidates.remove(kbe);
					return;
				}
				
				int depth = ThreadLocalRandom.current().nextInt(0, maxDepth);
				Key target = new Prefix(kbe.getID(), depth).createRandomKeyFromPrefix();
				
				SampleRequest req = new SampleRequest(target);
				
				 
				req.setDestination(kbe.getAddress());
				req.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT);
				req.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT);
				
				rpcCall(req, kbe.getID(), (c) -> {
					candidates.remove(kbe);
					visited.insert(ByteBuffer.wrap(kbe.getAddress().getAddress().getAddress()));
				});
								
			});
		}
		
	}

	@Override
	void callFinished(RPCCall c, MessageBase rsp) {
		if(!c.matchesExpectedID())
			return;
		
		SampleResponse sam = (SampleResponse) rsp;
		
		sam.getNodes(node.getDHT().getType()).entries().forEach(kbe -> {
			if(AddressUtils.isBogon(kbe.getAddress()))
				return;
			if(visited.probablyContains(ByteBuffer.wrap(kbe.getAddress().getAddress().getAddress())))
				return;
			candidates.add(kbe);
		});
		
		sam.getSamples().stream().forEach(k -> ihcallback.accept(c, k));
	}

	@Override
	void callTimeout(RPCCall c) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getTodoCount() {
		return candidates.size();
	}

	@Override
	protected boolean isDone() {
		return candidates.isEmpty();
	}
	
	@Override
	public void start() {
		
		maxDepth = node.table().stream().mapToInt(rte -> rte.prefix.getDepth()).max().getAsInt();
		
		node.table().stream().flatMap(rte -> rte.getBucket().entriesStream()).filter(KBucketEntry::eligibleForLocalLookup).forEach(kbe -> {
			candidates.add(kbe);
		});
		
		this.addListener(t -> {
			DHT.log("SamplingCrawl done " + visited.toString(), LogLevel.Info);
		});
		
		long size = node.getDHT().getEstimator().getEstimate();
		
		int bits = 1;
		
		while(bits < size * 3)
			bits <<= 1;
		
		visited = new GenericBloomFilter(bits, (int) size);
		
		super.start();
	}
	
	

}
