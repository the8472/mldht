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

import java.util.HashMap;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.KBucket;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;
import the8472.utils.concurrent.SerializedTaskExecutor;

/**
 * @author Damokles
 *
 */
public class PingRefreshTask extends Task {

	private boolean							cleanOnTimeout;
	boolean									alsoCheckGood	= false;
	boolean 								probeReplacement = false;
	private Map<MessageBase, KBucketEntry>	lookupMap;
	KBucket									bucket;

	/**
	 * @param rpc
	 * @param node
	 * @param bucket the bucket to refresh
	 * @param cleanOnTimeout if true Nodes that fail to respond are removed. should be false for normal use.
	 */
	public PingRefreshTask (RPCServer rpc, Node node, KBucket bucket, boolean cleanOnTimeout) {
		
		// TODO: remove target ID/make it optional?
		super(rpc.getDerivedID(),rpc, node);
		this.cleanOnTimeout = cleanOnTimeout;
		lookupMap = new HashMap<MessageBase, KBucketEntry>();
		
		addBucket(bucket);
	}
	
	public void checkGoodEntries(boolean val) {
		alsoCheckGood = val;
	}
	
	public void probeUnverifiedReplacement(boolean val) {
		probeReplacement = true;
	}
	
	public void addBucket(KBucket bucket) {
		if (bucket != null) {
			if(this.bucket !=null)
				new IllegalStateException("a bucket already present");
			this.bucket = bucket;
			bucket.updateRefreshTimer();
			for (KBucketEntry e : bucket.getEntries()) {
				if (e.needsPing() || cleanOnTimeout || alsoCheckGood) {
					todo.add(e);
				}
			}
			
			if(probeReplacement) {
				bucket.findPingableReplacement().ifPresent(todo::add);
			}
				
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callFinished(lbms.plugins.mldht.kad.RPCCallBase, lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	@Override
	void callFinished (RPCCall c, MessageBase rsp) {
		// most of the success handling is done by bucket maintenance
		synchronized (lookupMap) {
			KBucketEntry e = lookupMap.remove(c.getRequest());
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callTimeout(lbms.plugins.mldht.kad.RPCCallBase)
	 */
	@Override
	void callTimeout (RPCCall c) {
		MessageBase mb = c.getRequest();

		synchronized (lookupMap) {
			KBucketEntry e = lookupMap.remove(mb);
			if(e == null)
				return;
			
			KBucket bucket = node.table().entryForId(e.getID()).getBucket();
			if (bucket != null) {
				if(cleanOnTimeout) {
					DHT.logDebug("Removing invalid entry from cache.");
					bucket.removeEntryIfBad(e, true);
				}
			}
		}

	}
	
	
	final Runnable exclusiveUpdate = SerializedTaskExecutor.onceMore(() -> {
		if(todo.isEmpty()) {
			bucket.entriesStream().filter(KBucketEntry::needsPing).filter(e -> !hasVisited(e)).forEach(todo::add);
		}
		
		while(!todo.isEmpty() && canDoRequest()) {
			KBucketEntry e = todo.first();

			if (hasVisited(e) || (!alsoCheckGood && !e.needsPing())) {
				todo.remove(e);
				continue;
			}

			PingRequest pr = new PingRequest();
			pr.setDestination(e.getAddress());
			
			if(rpcCall(pr,e.getID(),c -> {
				c.builtFromEntry(e);
				synchronized (lookupMap) {
					lookupMap.put(pr, e);
				}
			})) {
				visited(e);
				todo.remove(e);
			}
				
		}
	});
	

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#update()
	 */
	@Override
	void update () {
		exclusiveUpdate.run();
	}
	
	@Override
	protected boolean isDone() {
		return todo.isEmpty() && getNumOutstandingRequests() == 0 && !isFinished();
	}
}
