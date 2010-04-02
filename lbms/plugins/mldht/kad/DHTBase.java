package lbms.plugins.mldht.kad;

import java.io.File;
import java.net.SocketException;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.ErrorMessage;
import lbms.plugins.mldht.kad.messages.FindNodeRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;
import lbms.plugins.mldht.kad.tasks.*;

/**
 * @author Damokles
 *
 */
public interface DHTBase {
	/**
	 * Start the DHT
	 */
	void start (DHTConfiguration config) throws SocketException;

	/**
	 * Stop the DHT
	 */
	void stop ();

	/**
	 * Update the DHT
	 */
	void update ();

	/**
	 * A Peer has received a PORT message, and uses this function to alert the DHT of it.
	 * @param ip The IP of the peer
	 * @param port The port in the PORT message
	 */
	void portRecieved (String ip, int port);

	/**
	 * Do an announce/scrape lookup on the DHT network
	 * @param info_hash The info_hash
	 * @return The task which handles this
	 */
	public PeerLookupTask lookupPeers(byte[] info_hash);
	
	
	/**
	 * Perform the put() operation for an announce
	 */
	public AnnounceTask announce(PeerLookupTask lookup, boolean isSeed, int btPort);

	/**
	 * See if the DHT is running.
	 */
	boolean isRunning ();

	/// Get statistics about the DHT
	DHTStats getStats ();

	/**
	 * Add a DHT node. This node shall be pinged immediately.
	 * @param host The hostname or ip
	 * @param hport The port of the host
	 */
	void addDHTNode (String host, int hport);

	void started ();

	void stopped ();

	public void ping (PingRequest r);

	public void findNode (FindNodeRequest r);

	public void response (MessageBase r);

	public void getPeers (GetPeersRequest r);

	public void announce (AnnounceRequest r);

	public void error (ErrorMessage r);

	public void timeout (MessageBase r);

	public void addStatsListener (DHTStatsListener listener);

	public void removeStatsListener (DHTStatsListener listener);

	public Node getNode ();

	public TaskManager getTaskManager ();

	boolean canStartTask ();

	NodeLookup findNode (Key id);

	PingRefreshTask refreshBucket (KBucket bucket);

	public PingRefreshTask refreshBuckets (List<RoutingTableEntry> buckets, boolean cleanOnTimeout);

	NodeLookup fillBucket (Key id, KBucket bucket);

	Key getOurID ();
}
