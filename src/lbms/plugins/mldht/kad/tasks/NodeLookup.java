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


import java.util.SortedSet;
import java.util.TreeSet;

import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.messages.*;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.kad.utils.PackUtil;

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
		addListener(new TaskListener() {
			public void finished(Task t) {
				done();
			}
		});
	}

	@Override
	void update () {
		// go over the todo list and send find node calls
		// until we have nothing left
		synchronized (todo) {

			while (canDoRequest() && validReponsesSinceLastClosestSetModification < DHTConstants.MAX_CONCURRENT_REQUESTS) {
				KBucketEntry e = todo.pollFirst();
				
				if(e == null)
					break;
				
				// only send a findNode if we haven't already visited the node
				if (hasVisited(e))
					continue;
				
					
				// send a findNode to the node
				FindNodeRequest fnr = new FindNodeRequest(targetKey);
				fnr.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT || DHT.getDHT(DHTtype.IPV4_DHT).getNode() != null && DHT.getDHT(DHTtype.IPV4_DHT).getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS);
				fnr.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT || DHT.getDHT(DHTtype.IPV6_DHT).getNode() != null && DHT.getDHT(DHTtype.IPV6_DHT).getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS);
				fnr.setDestination(e.getAddress());
				if(rpcCall(fnr,e.getID(),null))
					visited(e);
				else
					todo.add(e);
				
					
				// remove the entry from the todo list
			}
		}


	}
	
	@Override
	protected boolean isDone() {
		if (todo.size() == 0 && getNumOutstandingRequests() == 0 && !isFinished()) {
			return true;
		} else if (getNumOutstandingRequests() == 0 && validReponsesSinceLastClosestSetModification >= DHTConstants.MAX_CONCURRENT_REQUESTS) {
			return true; // quit after 10 nodes responsed
		}
		return false;
	}

	@Override
	void callFinished (RPCCall c, MessageBase rsp) {

		// check the response and see if it is a good one
		if (rsp.getMethod() == Method.FIND_NODE
				&& rsp.getType() == Type.RSP_MSG) {

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
				byte[] nodes = fnr.getNodes(type);
				if (nodes == null)
					continue;
				int nval = nodes.length / type.NODES_ENTRY_LENGTH;
				if (type == rpc.getDHT().getType())
				{
					synchronized (todo)
					{
						for (int i = 0; i < nval; i++)
						{
							// add node to todo list
							KBucketEntry e = PackUtil.UnpackBucketEntry(nodes, i * type.NODES_ENTRY_LENGTH, type);
							if (!AddressUtils.isBogon(e.getAddress()) && !node.allLocalIDs().contains(e.getID()) && !todo.contains(e) && !hasVisited(e))
							{
								todo.add(e);
							}
						}
					}
				} else
				{
					for (int i = 0; i < nval; i++)
					{
						KBucketEntry e = PackUtil.UnpackBucketEntry(nodes, i * type.NODES_ENTRY_LENGTH, type);
						DHT.getDHT(type).addDHTNode(e.getAddress().getAddress().getHostAddress(), e.getAddress().getPort());
					}
				}
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
		kns.fill();
		todo.addAll(kns.getEntries());
		

		super.start();
	}

	private void done () {
		synchronized (closestSet)
		{
			rpc.getDHT().getEstimator().update(closestSet,targetKey);
		}
	}
}
