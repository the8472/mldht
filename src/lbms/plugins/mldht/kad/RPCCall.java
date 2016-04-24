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
package lbms.plugins.mldht.kad;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;

/**
 * @author Damokles
 *
 */
public class RPCCall {

	private MessageBase				msg;
	private boolean					sourceWasKnownReachable;
	private boolean					stalled;
	private boolean					awaitingResponse;
	private List<RPCCallListener>	listeners		= new ArrayList<RPCCallListener>(3);
	private ScheduledFuture<?>		timeoutTimer;
	long					sentTime		= -1;
	long					responseTime	= -1;
	private Key						expectedID;
	long					expectedRTT = -1;
	

	public RPCCall (MessageBase msg) {
		assert(msg != null);
		this.msg = msg;
	}
	
	public RPCCall setExpectedID(Key id) {
		expectedID = id;
		return this;
	}
	
	public void builtFromEntry(KBucketEntry e) {
		sourceWasKnownReachable = e.verifiedReachable();
	}
	
	public boolean knownReachableAtCreationTime() {
		return sourceWasKnownReachable;
	}
	
	public RPCCall setExpectedRTT(long rtt) {
		expectedRTT = rtt;
		return this;
	}
	
	public long getExpectedRTT() {
		return expectedRTT;
	}
	
	public boolean matchesExpectedID(Key id) {
		return expectedID == null || id.equals(expectedID);
	}
	
	public Key getExpectedID() {
		return expectedID;
	}
	
	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCCallBase#response(lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	public void response (MessageBase rsp) {
		if (timeoutTimer != null) {
			timeoutTimer.cancel(false);
		}
		
		if(rsp.getType() == Type.RSP_MSG)
		{
			onCallResponse(rsp);
			return;
		}
		
		onCallTimeout();
		DHT.logError("received non-response ["+ rsp +"] in response to request: "+ msg.toString());
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCCallBase#addListener(lbms.plugins.mldht.kad.RPCCallListener)
	 */
	public RPCCall addListener (RPCCallListener cl) {
		if(awaitingResponse)
			throw new IllegalStateException("can only attach listeners while call is not started yet");
		if(cl != null)
			listeners.add(cl);
		return this;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCCallBase#getMessageMethod()
	 */
	public Method getMessageMethod () {
		return msg.getMethod();
	}

	/// Get the request sent
	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCCallBase#getRequest()
	 */
	public MessageBase getRequest () {
		return msg;
	}
	
	ScheduledExecutorService scheduler;

	void sent(RPCServer srv) {
		assert(expectedRTT > 0);
		assert(expectedRTT <= DHTConstants.RPC_CALL_TIMEOUT_MAX);
		awaitingResponse = true;
		sentTime = System.currentTimeMillis();
		
		scheduler = srv.getDHT().getScheduler();
		
		// spread out the stalls by +- 1ms to reduce lock contention
		int smear = ThreadLocalRandom.current().nextInt(-1000, 1000);
		timeoutTimer = scheduler.schedule(this::checkStallOrTimeout, expectedRTT*1000+smear, TimeUnit.MICROSECONDS);
	}
	
	void checkStallOrTimeout() {
		synchronized (this)
		{
			if(!awaitingResponse)
				return;
			// we stalled. for accurate measurement we still need to wait out the max timeout.
			// Start a new timer for the remaining time
			long elapsed = System.currentTimeMillis() - sentTime;
			long remaining = DHTConstants.RPC_CALL_TIMEOUT_MAX - elapsed;
			if(remaining > 0 && !stalled)
			{
				onStall();
				// re-schedule timer, we'll directly detect the timeout based on the stalled flag
				timeoutTimer = scheduler.schedule(this::checkStallOrTimeout, remaining, TimeUnit.MILLISECONDS);
			} else {
				onCallTimeout();
			}
		}
	}

	void sendFailed() {
		// fudge it, never sent it in the first place
		awaitingResponse = true;
		onCallTimeout();
	}

	private synchronized void onCallResponse (MessageBase rsp) {
		if(!awaitingResponse)
			return;
		awaitingResponse = false;
		responseTime = System.currentTimeMillis();
		
		if (listeners != null) {
			for (int i = 0; i < listeners.size(); i++) {
				try	{
					listeners.get(i).onResponse(this, rsp);
				} catch (Exception e) {
					DHT.log(e, LogLevel.Error);
				}
			}
		}
	}

	private synchronized void onCallTimeout () {
		if(!awaitingResponse)
			return;
		awaitingResponse = false;
		
		DHT.logDebug("RPCCall timed out ID: " + new String(msg.getMTID()));

		for (int i = 0; i < listeners.size(); i++) {
			try {
				listeners.get(i).onTimeout(this);
			} catch (Exception e) {
				DHT.log(e, LogLevel.Error);
			}
		}
	}
	
	private synchronized void onStall() {
		if(!awaitingResponse)
			return;
		if(stalled)
			return;
		stalled = true;
		
		DHT.logDebug("RPCCall stalled ID: " + new String(msg.getMTID()));
		if (listeners != null) {
			for (int i = 0; i < listeners.size(); i++) {
				try {
					listeners.get(i).onStall(this);
				} catch (Exception e) {
					DHT.log(e, LogLevel.Error);
				}
			}
		}
	}
	
	/**
	 * @return -1 if there is no response yet or it has timed out. The round trip time in milliseconds otherwise
	 */
	public long getRTT() {
		if(sentTime == -1 || responseTime == -1)
			return -1;
		return responseTime - sentTime;
	}
	
	public long getSentTime()
	{
		return sentTime;
	}
	
	public boolean wasStalled() {
		return stalled;
	}

}
