package lbms.plugins.mldht.kad;

import lbms.plugins.mldht.kad.messages.MessageBase;

/**
 *  Class which objects should derive from, if they want to know the result of a call.
 *
 * @author Damokles
 */
public interface RPCCallListener {

	/**
	 * A response was received.
	 * @param c The call
	 * @param rsp The response
	 */
	public void onResponse (RPCCallBase c, MessageBase rsp);
	
	
	/**
	 * The call has not timed out yet but is estimated to be unlikely to succeed
	 */
	public void onStall(RPCCallBase c);

	/**
	 * The call has timed out.
	 * @param c The call
	 */
	public void onTimeout (RPCCallBase c);
}
