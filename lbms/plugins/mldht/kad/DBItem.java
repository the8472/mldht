package lbms.plugins.mldht.kad;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Damokles
 *
 */
public class DBItem {

	protected byte[] item;
	private final long	time_stamp;

	private DBItem () {
		time_stamp = System.currentTimeMillis();
	}

	public DBItem (final byte[] ip_port) {
		this();
		item = ip_port.clone();
	}

	/// See if the item is expired
	public boolean expired (final long now) {
		return (now - time_stamp >= DHTConstants.MAX_ITEM_AGE);
	}

	/// Get the data of an item
	public byte[] getData () {
		return item;
	}

	@Override
	public String toString() {
		return "DBItem length:"+item.length;
	}

	@Override
	public boolean equals(final Object obj) {
		if(obj instanceof DBItem)
		{
			byte[] otherItem = ((DBItem)obj).item;
			return Arrays.equals(item, otherItem);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(item);
	}

	public static final Comparator<DBItem> ageOrdering = new Comparator<DBItem>() {
		public int compare(final DBItem o1, final DBItem o2) {
			return (int)(o1.time_stamp - o2.time_stamp);
		}
	};
}
