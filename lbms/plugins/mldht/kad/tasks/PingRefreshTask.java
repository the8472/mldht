package lbms.plugins.mldht.kad.tasks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;

/**
 * @author Damokles
 *
 */
public class PingRefreshTask extends Task {

	private boolean							cleanOnTimeout;
	private Map<MessageBase, KBucketEntry>	lookupMap;

	/**
	 * @param rpc
	 * @param node
	 * @param cleanOnTimeout if true Nodes that fail to respond are removed. should be false for normal use.
	 */
	public PingRefreshTask (RPCServerBase rpc, Node node, boolean cleanOnTimeout) {
		this(rpc, node, node.getBuckets(), cleanOnTimeout);
	}

	/**
	 * @param rpc
	 * @param node
	 * @param bucket the bucket to refresh
	 * @param cleanOnTimeout if true Nodes that fail to respond are removed. should be false for normal use.
	 */
	public PingRefreshTask (RPCServerBase rpc, Node node, KBucket bucket,
			boolean cleanOnTimeout) {
		super(node.getRootID(),rpc, node);
		this.cleanOnTimeout = cleanOnTimeout;
		if (cleanOnTimeout) {
			lookupMap = new HashMap<MessageBase, KBucketEntry>();
		}

		if (bucket != null) {
			for (KBucketEntry e : bucket.getEntries()) {
				if (e.isQuestionable() || cleanOnTimeout) {
					todo.add(e);
				}
			}
		}
	}

	/**
	 * @param rpc
	 * @param node
	 * @param bucket the bucket to refresh
	 * @param cleanOnTimeout if true Nodes that fail to respond are removed. should be false for normal use.
	 */
	public PingRefreshTask (RPCServerBase rpc, Node node, List<RoutingTableEntry> buckets,
			boolean cleanOnTimeout) {
		super(node.getRootID(), rpc, node,"Multi Bucket Refresh");
		this.cleanOnTimeout = cleanOnTimeout;
		if (cleanOnTimeout) {
			lookupMap = new HashMap<MessageBase, KBucketEntry>();
		}

		for (RoutingTableEntry tableEntry : buckets)
			for (KBucketEntry e : tableEntry.getBucket().getEntries())
				if (e.isQuestionable() || cleanOnTimeout)
					todo.add(e);
		
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callFinished(lbms.plugins.mldht.kad.RPCCallBase, lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	@Override
	void callFinished (RPCCallBase c, MessageBase rsp) {
		if (cleanOnTimeout) {
			synchronized (lookupMap) {
				lookupMap.remove(c.getRequest());
			}
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#callTimeout(lbms.plugins.mldht.kad.RPCCallBase)
	 */
	@Override
	void callTimeout (RPCCallBase c) {
		if (cleanOnTimeout) {
			MessageBase mb = c.getRequest();

			synchronized (lookupMap) {
				if (lookupMap.containsKey(mb)) {
					KBucketEntry e = lookupMap.remove(mb);
					
					KBucket bucket = node.findBucketForId(e.getID()).getBucket();
					if (bucket != null) {
						DHT.logDebug("Removing invalid entry from cache.");
						bucket.removeEntry(e, true);
					}
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.Task#update()
	 */
	@Override
	void update () {
		// go over the todo list and send ping
		// until we have nothing left
		synchronized (todo) {
			while (!todo.isEmpty() && canDoRequest()) {
				KBucketEntry e = todo.first();
				todo.remove(e);

				if (e.isGood()) {
					//Node responded in the meantime
					continue;
				}

				PingRequest pr = new PingRequest();
				pr.setDestination(e.getAddress());
				if (cleanOnTimeout) {
					synchronized (lookupMap) {
						lookupMap.put(pr, e);
					}
				}
				rpcCall(pr);
			}
		}

		if (todo.isEmpty() && getNumOutstandingRequests() == 0 && !isFinished()) {
			done();
		}
	}
}
