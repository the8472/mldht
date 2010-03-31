/**
 * 
 */
package lbms.plugins.mldht.kad.utils;

import java.util.Arrays;

public final class ByteWrapper {
	public final byte[] arr;
	private final int hash;
	
	public ByteWrapper(byte[] a)
	{
		arr = a;
		hash = Arrays.hashCode(a);
	}
	
	public ByteWrapper(short s)
	{
		this(new byte[] {(byte)(s>>8),(byte)(s&0xff)});
	}
	
	@Override
	public int hashCode() {
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj instanceof ByteWrapper && Arrays.equals(arr,((ByteWrapper)obj).arr);
	}
}