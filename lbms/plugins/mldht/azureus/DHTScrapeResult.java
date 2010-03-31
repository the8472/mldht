package lbms.plugins.mldht.azureus;

import java.net.MalformedURLException;
import java.net.URL;

import org.gudy.azureus2.plugins.download.Download;
import org.gudy.azureus2.plugins.download.DownloadScrapeResult;

public class DHTScrapeResult implements DownloadScrapeResult {
	
	private Download download;
	private int seedCount;
	private int peerCount;
	private long scrapeStartTime;
	
	public DHTScrapeResult(Download dl, int seeds, int peers) {
		download = dl;
		seedCount = seeds;
		peerCount = peers;
	}
	
	
	@Override
	public Download getDownload() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNextScrapeStartTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNonSeedCount() {
		return peerCount;
	}

	@Override
	public int getResponseType() {
		return DownloadScrapeResult.RT_SUCCESS;
	}

	void setScrapeStartTime(long time) {
		scrapeStartTime = time;
	}
	
	
	@Override
	public long getScrapeStartTime() {
		return scrapeStartTime;
	}

	@Override
	public int getSeedCount() {
		return seedCount;
	}

	@Override
	public String getStatus() {
		return null;
	}

	@Override
	public URL getURL() {
		try
		{
			return new URL("dht","mldht","scrape");
		} catch (MalformedURLException e)
		{
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void setNextScrapeStartTime(long nextScrapeStartTime) {
	// TODO Auto-generated method stub
	}
}
