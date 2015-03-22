package the8472.utils;

import java.nio.ByteBuffer;

public class Arrays {
	
	public static int compare(byte[] a, byte[] b) {
		return ByteBuffer.wrap(a).compareTo(ByteBuffer.wrap(b));
	}

}
