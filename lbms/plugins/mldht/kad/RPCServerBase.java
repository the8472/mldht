package lbms.plugins.mldht.kad;

import java.net.InetSocketAddress;

import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.utils.ResponseTimeoutFilter;

/**
 * @author Damokles
 *
 */
public interface RPCServerBase {

	public void start ();

	public void stop ();

	/**
	 * Do a RPC call.
	 * @param msg The message to send
	 * @return The call object
	 */
	public RPCCall doCall (MessageBase msg);

	/**
	 * Send a message, this only sends the message, it does not keep any call
	 * information. This should be used for replies.
	 * @param msg The message to send
	 */
	public void sendMessage (MessageBase msg);

	/**
	 * Ping a node, we don't care about the MTID.
	 * @param addr The address
	 */
	public void ping (InetSocketAddress addr);

	/**
	 * Find a RPC call, based on the mtid
	 * @param mtid The mtid
	 * @return The call
	 */
	public RPCCallBase findCall (byte[] mtid);

	/// Get the number of active calls
	public int getNumActiveRPCCalls ();

	public int getNumReceived ();

	public int getNumSent ();

	public RPCStats getStats ();

	public DHT getDHT();

	public ResponseTimeoutFilter getTimeoutFilter();
}