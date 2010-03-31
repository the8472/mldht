package lbms.plugins.mldht.kad;

import lbms.plugins.mldht.kad.utils.ByteWrapper;

/**
 * @author Damokles
 *
 */
public class KBucketEntryAndToken extends KBucketEntry {

	private ByteWrapper		token;

	public KBucketEntryAndToken (KBucketEntry kbe, byte[] token) {
		super(kbe);
		this.token = new ByteWrapper(token);
	}

	/**
	 * @return the token
	 */
	public byte[] getToken () {
		return token.arr;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.KBucketEntry#equals(java.lang.Object)
	 */
	@Override
	public boolean equals (Object obj) {
		if (obj instanceof KBucketEntryAndToken) {
			KBucketEntryAndToken kbet = (KBucketEntryAndToken) obj;
			if (super.equals(obj))
				return token.equals(kbet.token);
			return false;
		}
		return super.equals(obj);
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.KBucketEntry#hashCode()
	 */
	@Override
	public int hashCode () {
		return super.hashCode() ^ token.hashCode();
	}
}
