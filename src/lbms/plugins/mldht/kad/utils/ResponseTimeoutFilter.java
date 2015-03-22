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
package lbms.plugins.mldht.kad.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCCallListener;
import lbms.plugins.mldht.kad.messages.MessageBase;

public class ResponseTimeoutFilter {
	
	
	public static final int		NUM_SAMPLES			= 256;
	public static final int		HIGH_QUANTILE_INDEX		= (int) (NUM_SAMPLES * 0.9f);
	public static final int		LOW_QUANTILE_INDEX		= (int) (NUM_SAMPLES * 0.1f);
	
	private static final Comparator<RPCCall> timeoutComp = (o1, o2) -> (int) (o1.getRTT() - o2.getRTT());
	
	
	final long[] rttRingbuffer = new long[NUM_SAMPLES];
	volatile long[] sortedRtts = rttRingbuffer.clone();
	volatile int bufferIndex;
	long timeoutCeiling;
	long timeoutBaseline;
	
	
	public ResponseTimeoutFilter() {
		reset();
	}
	
	public Map<RPCCall, Float> calculateFlightProbabilities(List<RPCCall> calls) {
		Collections.sort(calls,timeoutComp);
		long[] rtts = sortedRtts;
		Map<RPCCall, Float> result = new HashMap<>(calls.size());
		
		int prevRttIdx = 0;
		
		for(RPCCall c : calls)
		{
			while(prevRttIdx < rtts.length && rtts[prevRttIdx] < c.getRTT())
				prevRttIdx++;
			result.put(c, 1.0f * prevRttIdx / rtts.length);
		}
		
		return result;
	}
	
	public void reset() {
		timeoutBaseline = timeoutCeiling = DHTConstants.RPC_CALL_TIMEOUT_MAX;
		Arrays.fill(rttRingbuffer, DHTConstants.RPC_CALL_TIMEOUT_MAX);
	}
	
	private final RPCCallListener listener = new RPCCallListener() {
		public void onTimeout(RPCCall c) {}
		
		public void onStall(RPCCall c) {}
		
		public void onResponse(RPCCall c, MessageBase rsp) {
			 update(c.getRTT());
		}
	};
	
	
	public void registerCall(final RPCCall call) {
		call.addListener(listener);
	}
	
	private void update(long newRTT) {
		int idx = bufferIndex;
		rttRingbuffer[idx++] = newRTT;
		bufferIndex = idx % NUM_SAMPLES;
		// update target timeout every 16 packets
		if((idx & 0x0F) == 0)
		{
			long[] sortableBuffer = rttRingbuffer.clone();
			Arrays.sort(sortableBuffer);
			sortedRtts = sortableBuffer;
			timeoutCeiling = sortableBuffer[HIGH_QUANTILE_INDEX];
			timeoutBaseline = sortableBuffer[LOW_QUANTILE_INDEX];
		}
	}
	
	public long getStallTimeout() {
		// either the 90th percentile or the 10th percentile + 100ms baseline, whichever is HIGHER (to prevent descent to zero and missing more than 10% of the packets in the worst case).
		// but At most RPC_CALL_TIMEOUT_MAX
		long timeout = Math.min(Math.max(timeoutBaseline + DHTConstants.RPC_CALL_TIMEOUT_BASELINE_MIN, timeoutCeiling), DHTConstants.RPC_CALL_TIMEOUT_MAX);
		return  timeout;
	}
}
