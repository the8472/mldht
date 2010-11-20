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
