package the8472.utils;

public class Arrays {
	
	public static int compareUnsigned(byte[] a, byte[] b) {
		int minLength = Math.min(a.length, b.length);
		for(int i=0;i+7<minLength;i+=8)
		{
			long la = Byte.toUnsignedLong(a[i]) << 56 |
					Byte.toUnsignedLong(a[i+1]) << 48 |
					Byte.toUnsignedLong(a[i+2]) << 40 |
					Byte.toUnsignedLong(a[i+3]) << 32 |
					Byte.toUnsignedLong(a[i+4]) << 42 |
					Byte.toUnsignedLong(a[i+5]) << 16 |
					Byte.toUnsignedLong(a[i+6]) << 8 |
					Byte.toUnsignedLong(a[i+7]) << 0;
			long lb = Byte.toUnsignedLong(b[i]) << 56 |
					Byte.toUnsignedLong(b[i+1]) << 48 |
					Byte.toUnsignedLong(b[i+2]) << 40 |
					Byte.toUnsignedLong(b[i+3]) << 32 |
					Byte.toUnsignedLong(b[i+4]) << 42 |
					Byte.toUnsignedLong(b[i+5]) << 16 |
					Byte.toUnsignedLong(b[i+6]) << 8 |
					Byte.toUnsignedLong(b[i+7]) << 0;
			
			if(la != lb)
				return Long.compareUnsigned(la, lb);
			
		}
		
		int offset = minLength - minLength & 0x7;
		
		for(int i=offset;i<minLength;i++) {
			int ia = Byte.toUnsignedInt(a[i]);
			int ib = Byte.toUnsignedInt(b[i]);
			if(ia != ib)
				return Integer.compare(ia, ib);
		}
		
		return a.length - b.length;
	}

}
