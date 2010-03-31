package lbms.plugins.mldht.azureus;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

import lbms.plugins.mldht.kad.DBItem;
import lbms.plugins.mldht.kad.PeerAddressDBItem;

import org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer;

/**
 * @author Damokles
 *
 */
public class DHTPeer implements DownloadAnnounceResultPeer {

	private static final String	PEER_SOURCE	= "DHT";

	private String				addr = "localhost";
	private int					port = 0;

	protected DHTPeer (PeerAddressDBItem item) {
		addr = item.getAddressAsString();
		port = item.getPort();
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getAddress()
	 */
	public String getAddress () {
		return addr;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getPeerID()
	 */
	public byte[] getPeerID () {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getPort()
	 */
	public int getPort () {
		return port;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getProtocol()
	 */
	public short getProtocol () {
		return DownloadAnnounceResultPeer.PROTOCOL_NORMAL;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getSource()
	 */
	public String getSource () {
		return PEER_SOURCE;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer#getUDPPort()
	 */
	public int getUDPPort () {
		return 0;
	}

}
