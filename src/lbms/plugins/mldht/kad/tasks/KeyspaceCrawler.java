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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.NodeList;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.FindNodeRequest;
import lbms.plugins.mldht.kad.messages.FindNodeResponse;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;

/**
 * @author The 8472
 *
 */
public class KeyspaceCrawler extends Task {
	
	Set<InetSocketAddress> responded = new HashSet<InetSocketAddress>();
	
	KeyspaceCrawler (RPCServer rpc, Node node) {
		super(Key.createRandomKey(),rpc, node);
		setInfo("Exhaustive Keyspace Crawl");
		addListener(t -> done());
	}

	@Override
	synchronized void update () {
		// go over the todo list and send find node calls
		// until we have nothing left
		synchronized (todo) {

			while (canDoRequest()) {
				KBucketEntry e;
				synchronized (todo)
				{
					e = todo.pollFirst();
				}
				
				// only send a findNode if we haven't allready visited the node
				if (hasVisited(e))
					continue;
				
				// send a findNode to the node
				FindNodeRequest fnr;

				fnr = new FindNodeRequest(Key.createRandomKey());
				fnr.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV4_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
				fnr.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV6_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
				fnr.setDestination(e.getAddress());
				rpcCall(fnr,e.getID(),null);


				if(canDoRequest())
				{
					fnr = new FindNodeRequest(e.getID());
					fnr.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV4_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
					fnr.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV6_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
					fnr.setDestination(e.getAddress());
					rpcCall(fnr,e.getID(),null);
				}

				visited(e);
				
			}
		}
	}

	@Override
	void callFinished (RPCCall c, MessageBase rsp) {
		if (isFinished()) {
			return;
		}

		// check the response and see if it is a good one
		if (rsp.getMethod() == Method.FIND_NODE
				&& rsp.getType() == Type.RSP_MSG) {

			FindNodeResponse fnr = (FindNodeResponse) rsp;
			
			responded.add(fnr.getOrigin());
			
			for (DHTtype type : DHTtype.values())
			{
				NodeList nodes = fnr.getNodes(type);
				if (nodes == null)
					continue;

				if (type == rpc.getDHT().getType())
				{
					synchronized (todo)
					{
						nodes.entries().forEach(e -> {
							if (!node.isLocalId(e.getID()) && !todo.contains(e) && !hasVisited(e))
								todo.add(e);
						});
					}
				} else
				{
					rpc.getDHT().getSiblings().stream().filter(sib -> sib.getType() == type).forEach(sib -> {
						nodes.entries().forEach(e -> {
							sib.addDHTNode(e.getAddress().getAddress().getHostAddress(), e.getAddress().getPort());
						});
					});
				}
			}


		}
	}
	
	@Override
	boolean canDoRequest() {
		return getNumOutstandingRequestsExcludingStalled() < DHTConstants.MAX_CONCURRENT_REQUESTS * 5;
	}
	
	@Override
	public
	void kill() {
		// do nothing to evade safeties
	}
	
	@Override
	protected boolean isDone() {
		if (todo.size() == 0 && getNumOutstandingRequests() == 0 && !isFinished()) {
			return true;
		}
		return false;
	}

	@Override
	void callTimeout (RPCCall c) {

	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#start()
	 */
	@Override
	public
	void start() {
		int added = 0;

		// delay the filling of the todo list until we actually start the task
		
		outer: for (RoutingTableEntry bucket : node.table().list())
			for (KBucketEntry e : bucket.getBucket().getEntries())
				if (e.eligibleForLocalLookup())
				{
					todo.add(e);
					added++;
				}
		super.start();
	}

	
	private void done () {
		System.out.println("crawler done, seen "+responded.size());
	}
}
