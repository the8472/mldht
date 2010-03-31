package lbms.plugins.mldht.kad;

/**
 * @author Damokles
 *
 */
public class DatabaseStats {
	private int	keyCount;
	private int	itemCount;

	/**
	 * @return the itemCount
	 */
	public int getItemCount () {
		return itemCount;
	}

	/**
	 * @return the keyCount
	 */
	public int getKeyCount () {
		return keyCount;
	}

	/**
	 * @param itemCount the itemCount to set
	 */
	protected void setItemCount (int itemCount) {
		this.itemCount = (itemCount >= 0) ? itemCount : 0;
	}

	/**
	 * @param keyCount the keyCount to set
	 */
	protected void setKeyCount (int keyCount) {
		this.keyCount = (keyCount >= 0) ? keyCount : 0;
	}
}
