package lbms.plugins.mldht.kad;

import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;

/**
 * @author Damokles
 *
 */
public interface RPCCallBase {

	/**
	 * Called when a queued call gets started. Starts the timeout timer.
	 */
	public void start ();

	/**
	 * Called by the server if a response is received.
	 * @param rsp
	 */
	public void response (MessageBase rsp);

	/**
	 * Add a listener for this call
	 * @param cl The listener
	 */
	public void addListener (RPCCallListener cl);

	/**
	 * Remove a listener for this call
	 * @param cl The listener
	 */
	public void removeListener (RPCCallListener cl);

	/**
	 * @return Message Method
	 */
	public Method getMessageMethod ();

	/// Get the request sent
	public MessageBase getRequest ();

	/**
	 * @return the queued
	 */
	public boolean isQueued ();

	/**
	 * @return -1 if there is no response yet or it has timed out. The round trip time in milliseconds otherwise
	 */
	public long getRTT();

	public boolean wasStalled();

}