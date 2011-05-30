package lbms.plugins.mldht.indexer;

import lbms.plugins.mldht.kad.Key;
import org.hibernate.Session;

public abstract class BatchQuery implements Runnable, Comparable<BatchQuery> {
	
	public BatchQuery(Key k) {
		key = k;
	}
	
	Session session;
	Key key;
	
	public int compareTo(BatchQuery o) {
		return key.compareTo(o.key);
	}
}