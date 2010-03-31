package lbms.plugins.mldht.azureus;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.gudy.azureus2.plugins.download.Download;

/**
 * @author Damokles
 *
 */
public class TrackedTorrent implements Delayed {

	private long		timestamp;
	private long		lastAnnounceStart;
	private Download	download;
	private boolean		announcing;

	public TrackedTorrent (Download download) {
		this.download = download;
	}

	public TrackedTorrent (Download download, long delay) {
		this.download = download;
		setDelay(delay);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
	 */
	public long getDelay (TimeUnit unit) {
		return unit.convert(timestamp - System.currentTimeMillis(),
				TimeUnit.MILLISECONDS);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo (TrackedTorrent o) {
		long x = getDelay(TimeUnit.MILLISECONDS)
				- o.getDelay(TimeUnit.MILLISECONDS);
		if (x > 0) {
			return 1;
		} else if (x < 0) {
			return -1;
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo (Delayed o) {
		long x = getDelay(TimeUnit.MILLISECONDS)
				- o.getDelay(TimeUnit.MILLISECONDS);
		if (x > 0) {
			return 1;
		} else if (x < 0) {
			return -1;
		}
		return 0;
	}

	/**
	 * @return the announcing
	 */
	public boolean isAnnouncing () {
		return announcing;
	}
	
	public boolean scrapeOnly() {
		return download.getState() == Download.ST_QUEUED;
	}

	/**
	 * @param announcing the announcing to set
	 */
	public void setAnnouncing (boolean announcing) {
		this.announcing = announcing;
	}

	public void setDelay (long delay) {
		timestamp = System.currentTimeMillis() + delay;

	}

	public void setDelay (long delay, TimeUnit unit) {
		timestamp = System.currentTimeMillis()
				+ TimeUnit.MILLISECONDS.convert(delay, unit);
	}

	/**
	 * @return the download
	 */
	public Download getDownload () {
		return download;
	}
	
	public long getLastAnnounceStart() {
		return lastAnnounceStart;
	}

	public void setLastAnnounceStart(long lastAnnounceStart) {
		this.lastAnnounceStart = lastAnnounceStart;
	}

}
