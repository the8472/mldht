package lbms.plugins.mldht.azureus;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.DBItem;
import lbms.plugins.mldht.kad.PeerAddressDBItem;

import org.gudy.azureus2.plugins.download.Download;
import org.gudy.azureus2.plugins.download.DownloadAnnounceResult;
import org.gudy.azureus2.plugins.download.DownloadAnnounceResultPeer;

/**
 * @author Damokles
 *
 */
public class DHTAnnounceResult implements DownloadAnnounceResult {

	private Download						dl;
	private Collection<PeerAddressDBItem>				peers;
	private DownloadAnnounceResultPeer[]	resultPeers;
	int delay;
	int scrapeSeeds;
	int scrapePeers;

	public DHTAnnounceResult (Download dl, Collection<PeerAddressDBItem> peers, int delay) {
		this.dl = dl;
		this.peers = peers;
		this.delay = delay;
	}

	/**
	 * Converts the DBItems into DHTPeers
	 */
	private void convertPeers () {
		resultPeers = new DownloadAnnounceResultPeer[peers.size()];
		
		
		int i = 0;		
		for(PeerAddressDBItem it : peers)
			resultPeers[i++] = new DHTPeer(it);
		
	}

	public void setScrapeSeeds(int scrapeSeeds) {
		this.scrapeSeeds = scrapeSeeds;
	}

	public void setScrapePeers(int scrapePeers) {
		this.scrapePeers = scrapePeers;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getDownload()
	 */
	public Download getDownload () {
		return dl;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getError()
	 */
	public String getError () {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getExtensions()
	 */
	public Map getExtensions () {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getNonSeedCount()
	 */
	public int getNonSeedCount () {
		return scrapePeers;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getPeers()
	 */
	public DownloadAnnounceResultPeer[] getPeers () {
		if (resultPeers == null) {
			convertPeers();
		}
		return resultPeers;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getReportedPeerCount()
	 */
	public int getReportedPeerCount () {
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getResponseType()
	 */
	public int getResponseType () {
		return DownloadAnnounceResult.RT_SUCCESS;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getSeedCount()
	 */
	public int getSeedCount () {
		return scrapeSeeds;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getTimeToWait()
	 */
	public long getTimeToWait () {
		return delay;
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.plugins.download.DownloadAnnounceResult#getURL()
	 */
	public URL getURL () {
		try
		{
			return new URL("dht","mldht","announce");
		} catch (MalformedURLException e)
		{
			e.printStackTrace();
			return null;
		}
	}

}
