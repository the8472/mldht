package lbms.plugins.mldht.kad.utils;

import java.util.Arrays;
import java.util.Collections;

import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCCallBase;
import lbms.plugins.mldht.kad.RPCCallListener;
import lbms.plugins.mldht.kad.messages.MessageBase;

public class ResponseTimeoutFilter {
	
	public static final int		NUM_SAMPLES			= 256;
	public static final int		QUANTILE_INDEX		= (int) (NUM_SAMPLES * 0.9f);
	
	
	final long[] rttRingbuffer = new long[NUM_SAMPLES];
	int bufferIndex;
	long targetTimeoutMillis;
	
	
	public ResponseTimeoutFilter() {
		reset();		
	}
	
	public void reset() {
		targetTimeoutMillis = DHTConstants.RPC_CALL_TIMEOUT_MAX;
		Arrays.fill(rttRingbuffer, DHTConstants.RPC_CALL_TIMEOUT_MAX);
	}
	
	
	public void registerCall(final RPCCallBase call) {
		call.addListener(new RPCCallListener() {
			public void onTimeout(RPCCallBase c) {}
			
			public void onStall(RPCCallBase c) {}
			
			public void onResponse(RPCCallBase c, MessageBase rsp) {
				 update(c.getRTT());
			}
		});
	}
	
	private void update(long newRTT) {
		rttRingbuffer[bufferIndex++] = newRTT;
		bufferIndex %= NUM_SAMPLES;
		long[] sortableBuffer = rttRingbuffer.clone();
		Arrays.sort(sortableBuffer);
		targetTimeoutMillis = sortableBuffer[QUANTILE_INDEX];
	}
	
	public long getStallTimeout() {
		long timeout = Math.max(DHTConstants.RPC_CALL_TIMEOUT_MIN, targetTimeoutMillis);
		//System.out.println(timeout);
		return  timeout;
	}
}
