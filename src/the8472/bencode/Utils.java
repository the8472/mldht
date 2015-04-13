package the8472.bencode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Utils {
	
	public static ByteBuffer str2buf(String str) {
		return ByteBuffer.wrap(str.getBytes(StandardCharsets.ISO_8859_1));
	}
	
	public static String buf2str(ByteBuffer buf) {
		byte[] arr = buf.array();
		return new String(arr,buf.arrayOffset() + buf.position(),buf.remaining(), StandardCharsets.ISO_8859_1);
	}
	
	public static byte[] buf2ary(ByteBuffer buf) {
		byte[] out = new byte[buf.remaining()];
		buf.get(out);
		return out;
	}
	
	public static byte[] str2ary(String str) {
		return str.getBytes(StandardCharsets.ISO_8859_1);
	}
	
	public static String prettyPrint(Object o) {
		StringBuilder b = new StringBuilder(1024);
		prettyPrintInternal(b, o);
		return b.toString();
	}
	
	private static void prettyPrintInternal(StringBuilder b, Object o) {
		if(o instanceof Map) {
			Map<Object,Object> m = (Map<Object, Object>) o;
			
			b.append("{");
			Iterator<Entry<Object,Object>> it = m.entrySet().iterator();
			while(it.hasNext()) {
				Map.Entry<?,?> e = it.next();
				prettyPrintInternal(b, e.getKey());
				b.append(":");
				prettyPrintInternal(b, e.getValue());
				if(it.hasNext())
					b.append(", ");
			}
			b.append("}");
			return;
		}
		
		if(o instanceof List) {
			List<?> l = (List<?>) o;
			b.append("[");
			Iterator<?> it = l.iterator();
			while(it.hasNext()) {
				Object e = it.next();
				prettyPrintInternal(b, e);
				if(it.hasNext())
					b.append(", ");
			}
			b.append("]");
			return;
		}
		
		if(o instanceof String) {
			b.append('"');
			b.append(o);
			b.append('"');
			return;
		}
		
		if(o instanceof Long || o instanceof Integer) {
			b.append(o);
			return;
		}
		
		if(o instanceof ByteBuffer) {
			ByteBuffer buf = ((ByteBuffer) o).slice();
			byte[] bytes;
			if(buf.hasArray() & buf.arrayOffset() == 0 && buf.capacity() == buf.limit())
				bytes = buf.array();
			else
				bytes = new byte[buf.remaining()];
			buf.get(bytes);
			o = bytes;
		}
		
		if(o instanceof byte[]) {
			byte[] bytes = (byte[]) o;
			if(bytes.length == 0) {
				b.append("\"\"");
				return;
			}
				
			
			if(bytes.length < 10) {
				b.append(stripToAscii(bytes));
				b.append('/');
			}
			b.append("0x");
			toHex(bytes, b, 20);
			
			if(bytes.length > 20) {
				b.append("...");
				b.append('(');
				b.append(bytes.length);
				b.append(')');
			}
			return;
		}
		
		b.append("unhandled type(").append(o).append(')');
	}
	
	public static String stripToAscii(byte[] arr) {
		return stripToAscii(ByteBuffer.wrap(arr));
	}
	
	public static String stripToAscii(ByteBuffer buf) {
		
		int length = buf.remaining();
				
		char[] out = new char[buf.remaining()];
		for(int i=0;i<length ;i++) {
			char b = (char)(buf.get(buf.position() + i) & 0xff);
			if(b < ' ' || b > '~')
				b = '�';
			out[i] = b;
		}
		return new String(out);
	}
	
	static void toHex(byte[] toHex, StringBuilder builder, int maxLength)
	{
		if(toHex.length < maxLength)
			maxLength = toHex.length;
		for (int i = 0; i < maxLength; i++) {
			int nibble = (toHex[i] & 0xF0) >> 4;
			builder.append((char)(nibble < 0x0A ? '0'+nibble : 'A'+nibble-10 ));
			nibble = toHex[i] & 0x0F;
			builder.append((char)(nibble < 0x0A ? '0'+nibble : 'A'+nibble-10 ));
		}
	}
	
	


}