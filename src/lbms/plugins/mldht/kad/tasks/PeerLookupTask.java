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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import lbms.plugins.mldht.kad.AnnounceNodeCache;
import lbms.plugins.mldht.kad.DBItem;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.KBucketEntryAndToken;
import lbms.plugins.mldht.kad.KClosestNodesSearch;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.ScrapeResponseHandler;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.GetPeersResponse;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.kad.utils.PackUtil;

/**
 * @author Damokles
 *
 */
public class PeerLookupTask extends Task {

	private boolean							noAnnounce;
	private boolean							lowPriority;
	private boolean							noSeeds;
	private boolean							fastTerminate;
	
	// nodes which have answered with tokens
	private List<KBucketEntryAndToken>		announceCanidates;
	private ScrapeResponseHandler			scrapeHandler;
	Consumer<PeerAddressDBItem>				resultHandler = (x) -> {};

	private Set<PeerAddressDBItem>			returnedItems;
	private SortedSet<KBucketEntryAndToken>	closestSet;
	
	AnnounceNodeCache						cache;



	public PeerLookupTask (RPCServer rpc, Node node,
			Key info_hash) {
		super(info_hash, rpc, node);
		announceCanidates = new ArrayList<KBucketEntryAndToken>(20);
		returnedItems = Collections.newSetFromMap(new ConcurrentHashMap<PeerAddressDBItem, Boolean>());

		this.closestSet = new TreeSet<KBucketEntryAndToken>(new KBucketEntry.DistanceOrder(targetKey));
		cache = rpc.getDHT().getCache();
		// register key even before the task is started so the cache can already accumulate entries
		cache.register(targetKey,false);

		DHT.logDebug("PeerLookupTask started: " + getTaskID());
		
		addListener(t -> done());
		
	}

	public void setScrapeHandler(ScrapeResponseHandler scrapeHandler) {
		this.scrapeHandler = scrapeHandler;
	}
	
	public void setResultHandler(Consumer<PeerAddressDBItem> handler) {
		resultHandler = handler;
	}
	
	public void setNoSeeds(boolean avoidSeeds) {
		noSeeds = avoidSeeds;
	}
	
	/**
	 * enabling this also enables noAnnounce
	 */
	public void setFastTerminate(boolean fastTerminate) {
		if(!isQueued())
			throw new IllegalStateException("cannot change lookup mode after startup");
		this.fastTerminate = fastTerminate;
		if(fastTerminate)
			setNoAnnounce(true);
	}
	
	public void setLowPriority(boolean lowPriority)
	{
		this.lowPriority = lowPriority;
	}

	public void setNoAnnounce(boolean noAnnounce) {
		this.noAnnounce = noAnnounce;
	}
	
	public boolean isNoAnnounce() {
		return noAnnounce;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callFinished(lbms.plugins.mldht.kad.RPCCall, lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	@Override
	void callFinished (RPCCall c, MessageBase rsp) {
		if (c.getMessageMethod() != Method.GET_PEERS) {
			return;
		}

		GetPeersResponse gpr = (GetPeersResponse) rsp;
		
		for (DHTtype type : DHTtype.values())
		{
			byte[] nodes = gpr.getNodes(type);
			if (nodes == null)
				continue;
			int nval = nodes.length / type.NODES_ENTRY_LENGTH;
			if (type == rpc.getDHT().getType())
			{
				synchronized (this)
				{
					for (int i = 0; i < nval; i++)
					{
						// add node to todo list
						KBucketEntry e = PackUtil.UnpackBucketEntry(nodes, i * type.NODES_ENTRY_LENGTH, type);
						if(!AddressUtils.isBogon(e.getAddress()) && !node.isLocalId(e.getID()) && !hasVisited(e))
							todo.add(e);
					}
				}

			} else
			{
				rpc.getDHT().getSiblings().stream().filter(sib -> sib.getType() == type).forEach(sib -> {
					for (int i = 0; i < nval; i++)
					{
						KBucketEntry e = PackUtil.UnpackBucketEntry(nodes, i * type.NODES_ENTRY_LENGTH, type);
						sib.addDHTNode(e.getAddress().getAddress().getHostAddress(), e.getAddress().getPort());
					}
				});
			}
		}

		List<DBItem> items = gpr.getPeerItems();
		//if(items.size() > 0)
		//	System.out.println("unique:"+new HashSet<DBItem>(items).size()+" all:"+items.size()+" ver:"+gpr.getVersion()+" entries:"+items);
		for (DBItem item : items)
		{
			if(!(item instanceof PeerAddressDBItem))
				continue;
			PeerAddressDBItem it = (PeerAddressDBItem) item;
			// also add the items to the returned_items list
			if(!AddressUtils.isBogon(it)) {
				resultHandler.accept(it);
				returnedItems.add(it);
			}
				
			
		}
		
		if(returnedItems.size() > 0 && firstResultTime == 0)
			firstResultTime = System.currentTimeMillis();
		
		KBucketEntry entry = new KBucketEntry(rsp.getOrigin(), rsp.getID());
		KBucketEntryAndToken toAdd = new KBucketEntryAndToken(entry, gpr.getToken());

		synchronized (this)
		{
			// if someone has peers he might have filters, collect for scrape
			if (!items.isEmpty() && scrapeHandler != null)
				scrapeHandler.addGetPeersRespone(gpr);
			
			// add the peer who responded to the closest nodes list, so we can do an announce
			if (gpr.getToken() != null)
				announceCanidates.add(toAdd);

			
			// if we scrape we don't care about tokens.
			// otherwise we're only done if we have found the closest nodes that also returned tokens
			if (noAnnounce || gpr.getToken() != null)
			{
				closestSet.add(toAdd);
				if (closestSet.size() > DHTConstants.MAX_ENTRIES_PER_BUCKET)
				{
					KBucketEntryAndToken last = closestSet.last();
					closestSet.remove(last);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callTimeout(lbms.plugins.mldht.kad.RPCCall)
	 */
	@Override
	void callTimeout (RPCCall c) {
	}
	
	@Override
	boolean canDoRequest() {
		if(lowPriority)
			return getNumOutstandingRequestsExcludingStalled() < DHTConstants.MAX_CONCURRENT_REQUESTS_LOWPRIO;
		return super.canDoRequest();
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#update()
	 */
	@Override
	void update () {
		// check if the cache has any closer nodes after the initial query
		Collection<KBucketEntry> cacheResults = cache.get(targetKey, lowPriority ? DHTConstants.MAX_CONCURRENT_REQUESTS_LOWPRIO : DHTConstants.MAX_CONCURRENT_REQUESTS);
		
		synchronized (this)
		{
			todo.addAll(cacheResults);
		}

		// go over the todo list and send get_peers requests
		// until we have nothing left
		while (canDoRequest() && !isClosestSetStable()) {
			KBucketEntry e;
			synchronized (this)
			{
				e = todo.pollFirst();
			}

			// only send a getPeers if we haven't already visited the node
			if (e == null || hasVisited(e))
				continue;
			
			// send a findNode to the node
			GetPeersRequest gpr = new GetPeersRequest(targetKey);
			gpr.setWant4(rpc.getDHT().getType() == DHTtype.IPV4_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV4_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
			gpr.setWant6(rpc.getDHT().getType() == DHTtype.IPV6_DHT || rpc.getDHT().getSiblings().stream().anyMatch(sib -> sib.getType() == DHTtype.IPV6_DHT && sib.getNode().getNumEntriesInRoutingTable() < DHTConstants.BOOTSTRAP_IF_LESS_THAN_X_PEERS));
			gpr.setDestination(e.getAddress());
			gpr.setScrape(true);
			gpr.setNoSeeds(noSeeds);
			if(rpcCall(gpr, e.getID(), call -> {
				call.addListener(cache.getRPCListener());
				long rtt = e.getRTT();
				if(rtt < DHTConstants.RPC_CALL_TIMEOUT_MAX)
					call.setExpectedRTT(rtt);
			})) {
				visited(e);
			} else {
				synchronized (this)
				{
					todo.add(e);
				}
			}
		}
		

	}
	
	private synchronized boolean isClosestSetStable() {
		if(todo.isEmpty())
			return true;
		return closestSet.size() >= DHTConstants.MAX_ENTRIES_PER_BUCKET && targetKey.threeWayDistance(todo.first().getID(), closestSet.last().getID()) > 0;
	}
	
	@Override
	protected boolean isDone() {
		int waitingFor = fastTerminate ? getNumOutstandingRequestsExcludingStalled() : getNumOutstandingRequests();
		
		if (todo.isEmpty() && waitingFor == 0) {
			return true;
		}
		
		return waitingFor == 0 && isClosestSetStable();
	}

	private void done() {

		synchronized (this)
		{
			// feed the estimator if we have usable results
			if(!todo.isEmpty() && isClosestSetStable())
			{
				SortedSet<Key> toEstimate = new TreeSet<Key>();
				for(KBucketEntryAndToken e : closestSet)
					toEstimate.add(e.getID());
				rpc.getDHT().getEstimator().update(toEstimate,targetKey);
			}
			
		}
	
		//System.out.println(returned_items);
		//System.out.println("overall:"+returnedItems.size());
	}
	
	public List<KBucketEntryAndToken> getAnnounceCanidates() {
		if(fastTerminate || noAnnounce)
			throw new IllegalStateException("cannot use fast lookups for announces");
		return announceCanidates;
	}


	/**
	 * @return the returned_items
	 */
	public Set<PeerAddressDBItem> getReturnedItems () {
		return Collections.unmodifiableSet(returnedItems);
	}

	/**
	 * @return the info_hash
	 */
	public Key getInfoHash () {
		return targetKey;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#start()
	 */
	@Override
	public void start () {
		//delay the filling of the todo list until we actually start the task
		KClosestNodesSearch kns = new KClosestNodesSearch(targetKey,
				DHTConstants.MAX_ENTRIES_PER_BUCKET * 4,rpc.getDHT());

		kns.fill();
		todo.addAll(kns.getEntries());
		
		// re-register once we actually started
		cache.register(targetKey,fastTerminate);
		todo.addAll(cache.get(targetKey,DHTConstants.MAX_CONCURRENT_REQUESTS * 2));

		super.start();
	}
}
