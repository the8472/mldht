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

import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;

/**
 * @author Damokles
 *
 */
public class RPCStats {

	private long	receivedBytes;
	private long	sentBytes;

	private long	tmpReceivedBytes;
	private long	tmpSentBytes;
	private long	receivedBytesPerSec;
	private long	sentBytesPerSec;
	private long	tmpReceivedTimestamp;
	private long	tmpSentTimestamp;

	private int[][]	sentMessages;
	private int[][]	receivedMessages;
	private int[]	timeoutMessages;

	protected RPCStats () {
		sentMessages = new int[Method.values().length][Type.values().length];
		receivedMessages = new int[Method.values().length][Type.values().length];
		timeoutMessages = new int[Method.values().length];
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("### local RPCs\n");
		b.append("REQ / RSP / Timeout\n");
		for(Method m : Method.values())
		{
			b.append(m).append('\t').append(sentMessages[m.ordinal()][Type.REQ_MSG.ordinal()]).append('/').append(receivedMessages[m.ordinal()][Type.RSP_MSG.ordinal()]).append('/').append(timeoutMessages[m.ordinal()]).append('\n');
		}
		b.append("### remote RPCs\n");
		b.append("REQ / RSP\n");
		for(Method m : Method.values())
		{
			b.append(m).append('\t').append(receivedMessages[m.ordinal()][Type.REQ_MSG.ordinal()]).append('/').append(sentMessages[m.ordinal()][Type.RSP_MSG.ordinal()]).append('\n');
		}

		
		return b.toString();
	}

	/**
	 * @return the receivedBytes
	 */
	public long getReceivedBytes () {
		return receivedBytes;
	}

	/**
	 * @return the sentBytes
	 */
	public long getSentBytes () {
		return sentBytes;
	}

	/**
	 * @return
	 */
	public long getReceivedBytesPerSec () {
		long now = System.currentTimeMillis();
		long d = now - tmpReceivedTimestamp;
		if (d > 950) {
			receivedBytesPerSec = (int) (tmpReceivedBytes * 1000 / d);
			tmpReceivedBytes = 0;
			tmpReceivedTimestamp = now;
		}
		return receivedBytesPerSec;
	}

	/**
	 * @return
	 */
	public long getSentBytesPerSec () {
		long now = System.currentTimeMillis();
		long d = now - tmpSentTimestamp;
		if (d > 950) {
			sentBytesPerSec = (int) (tmpSentBytes * 1000 / d);
			tmpSentBytes = 0;
			tmpSentTimestamp = now;
		}
		return sentBytesPerSec;
	}

	/**
	 * Returns the Count for the specified Message
	 *
	 * @param m The method of the message
	 * @param t The type of the message
	 * @return count
	 */
	public int getSentMessageCount (Method m, Type t) {
		return sentMessages[m.ordinal()][t.ordinal()];
	}

	/**
	 * Returns the Count for the specified Message
	 *
	 * @param m The method of the message
	 * @param t The type of the message
	 * @return count
	 */
	public int getReceivedMessageCount (Method m, Type t) {
		return receivedMessages[m.ordinal()][t.ordinal()];
	}

	/**
	 * Returns the Count for the specified requests
	 *
	 * @param m The method of the message
	 * @return count
	 */
	public int getTimeoutMessageCount (Method m) {
		return timeoutMessages[m.ordinal()];
	}

	/**
	 * @param receivedBytes the receivedBytes to add
	 */
	protected void addReceivedBytes (long receivedBytes) {
		tmpReceivedBytes += receivedBytes;
		this.receivedBytes += receivedBytes;
	}

	/**
	 * @param sentBytes the sentBytes to add
	 */
	protected void addSentBytes (long sentBytes) {
		tmpSentBytes += sentBytes;
		this.sentBytes += sentBytes;
	}

	protected void addSentMessageToCount (MessageBase msg) {
		sentMessages[msg.getMethod().ordinal()][msg.getType().ordinal()]++;
	}

	protected void addSentMessageToCount (Method m, Type t) {
		sentMessages[m.ordinal()][t.ordinal()]++;
	}

	protected void addReceivedMessageToCount (MessageBase msg) {
		receivedMessages[msg.getMethod().ordinal()][msg.getType().ordinal()]++;
	}

	protected void addReceivedMessageToCount (Method m, Type t) {
		receivedMessages[m.ordinal()][t.ordinal()]++;
	}

	protected void addTimeoutMessageToCount (MessageBase msg) {
		timeoutMessages[msg.getMethod().ordinal()]++;
	}
}
