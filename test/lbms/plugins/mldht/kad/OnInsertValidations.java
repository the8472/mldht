package lbms.plugins.mldht.kad;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static the8472.utils.Functional.unchecked;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Test;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.messages.PingRequest;
import lbms.plugins.mldht.kad.messages.PingResponse;
import the8472.utils.io.NetMask;

public class OnInsertValidations {
	
	Node node;
	Key nodeId;
	
	@Before
	public void setup() {
		DHT dht = new DHT(DHTtype.IPV6_DHT);
		node = new Node(dht);
		nodeId = node.registerServer(null);
	}
	
	private InetAddress generateIp(byte subnet) {
		// generate valid unicast IPs from 2001:20xx::/32
		byte[] addr = new byte[16];
		ThreadLocalRandom.current().nextBytes(addr);
		addr[0] = 0x20;
		addr[1] = 0x01;
		addr[2] = 0x20;
		addr[3] = subnet;

		return unchecked(() -> InetAddress.getByAddress(addr));
	}
	
	private void fillTable() {
		for(int i=0;i<1000;i++) {
			node.insertEntry(new KBucketEntry(new InetSocketAddress(generateIp((byte)0x00), 1024), Key.createRandomKey()), true);
		}
		node.rebuildAddressCache();
	}

	@Test
	public void testImmediateEvictionOnIdMismatch() {
		fillTable();
		KBucket bucket = node.getBuckets().get(0).getBucket();
		KBucketEntry entry = bucket.randomEntry().get();
		
		PingRequest req = new PingRequest();
		RPCCall c = new RPCCall(req);
		c.setExpectedID(entry.getID());
		PingResponse rsp = new PingResponse(new byte[0]);
		rsp.setOrigin(entry.getAddress());
		rsp.setID(Key.createRandomKey());
		rsp.setAssociatedCall(c);
		node.recieved(rsp);

		assertFalse(bucket.findByIPorID(entry.getAddress().getAddress(), entry.getID()).isPresent());
		assertFalse(bucket.findByIPorID(null, rsp.getID()).isPresent());
	}
	
	@Test
	public void testRTTPreference() {
		fillTable();
		
		Collection<Key> localIds = node.localIDs();
		
				
		RoutingTableEntry nonLocalFullBucket = node.getBuckets().stream().filter(b -> b.getBucket().getNumReplacements() == DHTConstants.MAX_ENTRIES_PER_BUCKET && localIds.stream().noneMatch(k -> b.prefix.isPrefixOf(k))).findAny().get();
		
		PingRequest req = new PingRequest();
		RPCCall call = new RPCCall(req);
		PingResponse rsp = new PingResponse(new byte[0]);
		rsp.setAssociatedCall(call);
		Key newId = nonLocalFullBucket.prefix.createRandomKeyFromPrefix();
		rsp.setID(newId);
		rsp.setOrigin(new InetSocketAddress(generateIp((byte)0x00), 1234));
		
		node.recieved(rsp);
		
		// doesn't get inserted because the replacement buckets only overwrite entries once every second
		assertFalse(nonLocalFullBucket.getBucket().getReplacementEntries().stream().anyMatch(e -> e.getID().equals(newId)));
		
		long now = System.currentTimeMillis();
		call.sentTime = now - 50;
		call.responseTime = now;
		
		node.recieved(rsp);
		
		// the once-per-second rule does not apply if the new entry has a lower RTT than the existing value
		assertTrue(nonLocalFullBucket.getBucket().getReplacementEntries().stream().anyMatch(e -> e.getID().equals(newId)));
	}
	
	@Test
	public void testTrustedNodes() {
		fillTable();
		
		Collection<Key> localIds = node.localIDs();
		
		RoutingTableEntry nonLocalFullBucket = node.getBuckets().stream().filter(e -> e.getBucket().getNumEntries() == DHTConstants.MAX_ENTRIES_PER_BUCKET && localIds.stream().noneMatch(k -> e.prefix.isPrefixOf(k))).findAny().get();
		
		PingRequest req = new PingRequest();
		RPCCall call = new RPCCall(req);
		PingResponse rsp = new PingResponse(new byte[0]);
		rsp.setAssociatedCall(call);
		Key newId = nonLocalFullBucket.prefix.createRandomKeyFromPrefix();
		rsp.setID(newId);
		rsp.setOrigin(new InetSocketAddress(generateIp((byte)0x02), 1234));
		
		node.recieved(rsp);
		assertFalse(node.getBuckets().stream().anyMatch(e -> e.getBucket().findByIPorID(null, newId).isPresent()));
		
		byte[] addr = new byte[16];
		addr[0] = 0x20;
		addr[1] = 0x01;
		addr[2] = 0x20;
		addr[3] = 0x02;
		
		NetMask mask = new NetMask(unchecked(() -> InetAddress.getByAddress(addr)), 32);
		
		node.setTrustedNetMasks(Collections.singleton(mask));
		
		node.recieved(rsp);
		assertTrue(node.getBuckets().stream().anyMatch(e -> e.getBucket().findByIPorID(null, newId).isPresent()));
		
	}
}
