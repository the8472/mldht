/*
 *    This file is part of mlDHT.
 * 
 *    mlDHT is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 2 of the License, or
 *    (at your option) any later version.
 * 
 *    mlDHT is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 * 
 *    You should have received a copy of the GNU General Public License
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>.
 */
package lbms.plugins.mldht.kad.tasks;


import static the8472.utils.Functional.sync;

import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.KClosestNodesSearch;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.NodeList;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.FindNodeRequest;
import lbms.plugins.mldht.kad.messages.FindNodeResponse;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import the8472.utils.concurrent.SerializedTaskExecutor;

/**
 * @author Damokles
 *
 */
public class NodeLookup extends Task {
	private int						validReponsesSinceLastClosestSetModification;
	SortedSet<Key>			closestSet;
	private boolean forBootstrap = false;
	
	public NodeLookup (Key node_id, RPCServer rpc, Node node, boolean isBootstrap) {
		super(node_id, rpc, node);
		forBootstrap = isBootstrap;
		this.closestSet = new TreeSet<Key>(new Key.DistanceOrder(targetKey));
		addListener(t -> updatedPopulationEstimates());
	}
	
	final Runnable exclusiveUpdate = SerializedTaskExecutor.onceMore(() -> {
		while(!todo.isEmpty() && canDoRequest() && !isClosestSetStable() && !nextTodoUseless()) {
			KBucketEntry e = todo.first();
			
			if(e == null)
				continue;
			
			// only send a findNode if we haven't already visited the node
			if (hasVisited(e)) {
				todo.remove(e);
				continue;
			}
				
			// send a findNode to the node
			FindNodeRequest fnr = new FindNodeRequest(targetKey);
			fnr.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV4_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
			fnr.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV6_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
			fnr.setDestination(e.getAddress());
			if(rpcCall(fnr,e.getID(), (call) -> {
				long rtt = e.getRTT();
				rtt = rtt + rtt / 2; // *1.5 since this is the average and not the 90th percentile like the timeout filter
				if(rtt < DHTConstants.RPC_CALL_TIMEOUT_MAX && rtt < rpc.getTimeoutFilter().getStallTimeout())
					call.setExpectedRTT(rtt); // only set a node-specific timeout if it's better than what the server would apply anyway
			})) {
				visited(e);
				todo.remove(e);
			}
		}
	});

	@Override
	void update () {
		exclusiveUpdate.run();
	}
	
	/**
	 * avoid backtracking if the next request would be due to stall detection
	 */
	private boolean nextTodoUseless() {
		if(getNumOutstandingRequests() < DHTConstants.MAX_CONCURRENT_REQUESTS)
			return false;
		
		
		
		Key nextTodo = null;
		
		try {
			nextTodo = todo.first().getID();
		} catch(NoSuchElementException ex) {
			return true;
		}
		
		Key furthestFromTarget = targetKey.distance(Key.MAX_KEY);
		
		Key closest = sync(closestSet, s -> s.isEmpty() ? furthestFromTarget : s.first());
		Key furthest = sync(closestSet, s -> s.isEmpty() ? furthestFromTarget : s.last());
	
		// all in-flight requests stalled -> try anything that might improve the closest set
		// TODO: faster backtracking strategy for closest set stabilization. currently relies on full timeouts (track front and tail stability of closest set?)
		if(getNumOutstandingRequestsExcludingStalled() == 0 && targetKey.threeWayDistance(nextTodo, furthest) < 0)
			return false;
		// immediately try things that are closer to the target than anything we've seen
		if(targetKey.threeWayDistance(nextTodo, closest) < 0)
			return false;

		return true;
	}
	
	private boolean isClosestSetStable() {
		synchronized (closestSet) {
			if(closestSet.size() < DHTConstants.MAX_ENTRIES_PER_BUCKET)
				return false;
			if(validReponsesSinceLastClosestSetModification < DHTConstants.MAX_CONCURRENT_REQUESTS)
				return false;
			boolean haveBetterTodosForClosestSet = !todo.isEmpty() && targetKey.threeWayDistance(todo.first().getID(), closestSet.last()) < 0;
			return !haveBetterTodosForClosestSet;
		}
	}
	
	@Override
	protected boolean isDone() {
		if (getNumOutstandingRequests() == 0 && todo.isEmpty() && !isFinished()) {
			return true;
		} else if (getNumOutstandingRequests() == 0 && isClosestSetStable()) {
			return true; // quit after 10 nodes responsed
		}
		return false;
	}

	@Override
	void callFinished (RPCCall c, MessageBase rsp) {

		// check the response and see if it is a good one
		if (rsp.getMethod() != Method.FIND_NODE || rsp.getType() != Type.RSP_MSG)
			return;

		synchronized (closestSet) {
			Key toAdd = rsp.getID();
			closestSet.add(toAdd);
			if (closestSet.size() > DHTConstants.MAX_ENTRIES_PER_BUCKET) {
				Key last = closestSet.last();
				closestSet.remove(last);
				if (toAdd == last) {
					validReponsesSinceLastClosestSetModification++;
				} else {
					validReponsesSinceLastClosestSetModification = 0;
				}
			}
		}

		FindNodeResponse fnr = (FindNodeResponse) rsp;

		for (DHTtype type : DHTtype.values())
		{
			NodeList nodes = fnr.getNodes(type);
			if (nodes == null)
				continue;
			if (type == rpc.getDHT().getType()) {
				nodes.entries().forEach(e -> {
					if (!AddressUtils.isBogon(e.getAddress()) && !node.isLocalId(e.getID()) && !todo.contains(e) && !hasVisited(e))
						todo.add(e);
					
				});
			} else {
				rpc.getDHT().getSiblings().stream().filter(sib -> sib.getType() == type).forEach(sib -> {
					nodes.entries().forEach(e -> {
						sib.addDHTNode(e.getAddress().getAddress().getHostAddress(), e.getAddress().getPort());
					});
				});
			}
		}

	}

	@Override
	void callTimeout (RPCCall c) {

	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#start()
	 */
	@Override
	public
	void start () {

		// if we're bootstrapping start from the bucket that has the greatest possible distance from ourselves so we discover new things along the (longer) path
		Key knsTargetKey = forBootstrap ? targetKey.distance(Key.MAX_KEY) : targetKey;
		
		// delay the filling of the todo list until we actually start the task
		KClosestNodesSearch kns = new KClosestNodesSearch(knsTargetKey, 3 * DHTConstants.MAX_ENTRIES_PER_BUCKET, rpc.getDHT());
		kns.filter = KBucketEntry::eligibleForLocalLookup;
		kns.fill();
		todo.addAll(kns.getEntries());
		

		super.start();
	}

	private void updatedPopulationEstimates () {
		synchronized (closestSet)
		{
			rpc.getDHT().getEstimator().update(closestSet,targetKey);
		}
	}
}
