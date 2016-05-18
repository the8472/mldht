package lbms.plugins.mldht.kad.tasks;

import static java.lang.Math.max;

import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCState;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/*
 * Issues:
 * 
 * - spurious packet loss
 * - remotes might fake IDs. possibly with collusion.
 * - invalid results
 *   - duplicate IPs
 *   - duplicate IDs
 *   - wrong IDs -> might trip up fake ID detection!
 *   - IPs not belonging to DHT nodes -> DoS abuse
 * 
 * Solution:
 * 
 * - generally avoid querying an IP more than once
 * - dedup result lists from each node
 * - ignore responses with unexpected IDs. normally this could be abused to silence others, but...
 * - allow duplicate requests if many *separate* sources suggest precisely the same <id, ip, port> tuple
 * 
 * -> we can recover from all the above-listed issues because the terminal set of nodes should have some partial agreement about their neighbors
 * 
 * 
 * 
 */
public class IterativeLookupCandidates {
	
	Key target;
	Map<KBucketEntry, Set<KBucketEntry>> candidates;
	// maybe split out call tracking
	Map<RPCCall, KBucketEntry> calls;
	Collection<Object> accepted;
	
	public IterativeLookupCandidates(Key target) {
		this.target = target;
		calls = new ConcurrentHashMap<>();
		candidates = new ConcurrentHashMap<>();
		accepted = new HashSet<>();
						
	}
	
	void addCall(RPCCall c, KBucketEntry kbe) {
		synchronized (this) {
			calls.put(c, kbe);
		}
	}
	
	KBucketEntry acceptResponse(RPCCall c) {
		// we ignore on mismatch, node will get a 2nd chance if sourced from multiple nodes and hasn't sent a successful reply yet
		synchronized (this) {
			if(!c.matchesExpectedID())
				return null;
			
			KBucketEntry kbe = calls.get(c);
			
			boolean insertOk = !accepted.contains(kbe.getAddress().getAddress()) && !accepted.contains(kbe.getID());
			if(insertOk) {
				accepted.add(kbe.getAddress().getAddress());
				accepted.add(kbe.getID());
				return kbe;
			}
			return null;
		}
	}
	
	void addCandidates(KBucketEntry source, Collection<KBucketEntry> entries) {
		Set<Object> dedup = new HashSet<>();
		
		for(KBucketEntry e : entries) {
			if(!dedup.add(e.getID()) || !dedup.add(e.getAddress().getAddress()))
				continue;
			
			synchronized (this) {
				Set<KBucketEntry> set = candidates.compute(e, (unused, s) -> s == null ? new HashSet<>() : s);
				if(source != null)
					set.add(source);
			}
			
		}
		
		
	}
	
	Comparator<Map.Entry<KBucketEntry, Set<KBucketEntry>>> comp() {
		Comparator<KBucketEntry> d = new KBucketEntry.DistanceOrder(target);
		Comparator<Set<KBucketEntry>> s = (a, b) -> b.size() - a.size();
		return Map.Entry.<KBucketEntry, Set<KBucketEntry>>comparingByKey(d).thenComparing(Map.Entry.comparingByValue(s));
	}
	
	Optional<KBucketEntry> next() {
		synchronized (this) {
			return cand().min(comp()).map(Map.Entry::getKey);
		}
	}
	
	Stream<Map.Entry<KBucketEntry, Set<KBucketEntry>>> cand() {
		return candidates.entrySet().stream().filter(me -> {
			KBucketEntry kbe = me.getKey();
			InetAddress addr = kbe.getAddress().getAddress();
			
			if(accepted.contains(addr) || accepted.contains(kbe.getID()))
				return false;
			
			int dups = 0;
			for(RPCCall c : calls.keySet()) {
				if(!addr.equals(c.getRequest().getDestination().getAddress()))
					continue;
				
				// in flight, not stalled
				if(c.state() == RPCState.SENT || c.state() == RPCState.UNSENT)
					return false;
				
				// already got a response from that addr that does not match what we would expect from this candidate anyway
				if(c.state() == RPCState.RESPONDED && !c.getResponse().getID().equals(me.getKey().getID()))
					return false;
				// we don't strictly check the presence of IDs in error messages, so we can't compare those here
				if(c.state() == RPCState.ERROR)
					return false;
				dups++;
			}
			// log2 scale
			int sources = 31 - Integer.numberOfLeadingZeros(max(me.getValue().size(), 1));
			//System.out.println("sd:" + sources + " " + dups);
			
			return sources >= dups;
		});
	}
	
	Stream<Map.Entry<KBucketEntry, Set<KBucketEntry>>> allCand() {
		return candidates.entrySet().stream();
	}
	
	int numCalls(KBucketEntry kbe) {
		return (int) calls.entrySet().stream().filter(me -> me.getValue().equals(kbe)).count();
	}
	
	int numRsps(KBucketEntry kbe) {
		return (int) calls.keySet().stream().filter(c -> c.state() == RPCState.RESPONDED && c.getResponse().getID().equals(kbe.getID()) && c.getResponse().getOrigin().equals(kbe.getAddress())).count();
	}

}
