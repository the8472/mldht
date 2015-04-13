package the8472.bencode;

import static the8472.bencode.Utils.str2buf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class BEncoder {
	
	private ByteBuffer buf;
	
	public static class RawData {
		ByteBuffer rawBuf;
		
		public RawData(ByteBuffer b) {
			rawBuf = b;
		}
	}
	
	public ByteBuffer encode(Map<String, Object> toEnc, int maxSize) {
		buf = ByteBuffer.allocate(maxSize);
		encodeInternal(toEnc);
		buf.flip();
		return buf;
	}
	
	private void encodeInternal(Object o) {
		if(o instanceof Map) {
			encodeMap((Map<String, Object>) o);
			return;
		}
		
		if(o instanceof List) {
			encodeList((List<Object>) o);
			return;
		}
		
		if(o instanceof byte[]) {
			o = ByteBuffer.wrap((byte[]) o);
		}
		
		if(o instanceof String) {
			o = str2buf((String)o);
		}
		
		if(o instanceof ByteBuffer) {
			ByteBuffer clone = ((ByteBuffer) o).slice();
			encodeLong(clone.remaining(), ':');
			buf.put(clone);
			return;
		}
		
		if(o instanceof Integer) {
			buf.put((byte) 'i');
			encodeLong(((Integer) o).longValue(),'e');
			return;
		}
		
		if(o instanceof Long) {
			buf.put((byte) 'i');
			encodeLong(((Long) o).longValue(), 'e');
			return;
		}
		
		if(o instanceof RawData) {
			buf.put(((RawData) o).rawBuf);
			return;
		}
		
		throw new RuntimeException("unknown object to encode " + o);
	}
	
	private void encodeList(List<Object> l) {
		buf.put((byte) 'l');
		l.stream().forEachOrdered(e -> encodeInternal(e));
		buf.put((byte) 'e');
	}
	
	private void encodeMap(Map<String, Object> map) {
		buf.put((byte) 'd');
		map.entrySet().stream().sorted((a, b) -> a.getKey().compareTo(b.getKey())).forEachOrdered(e -> {
			encodeInternal(e.getKey());
			encodeInternal(e.getValue());
		});
		buf.put((byte) 'e');
	}
	
	private void encodeLong(long val, char terminator) {
		buf.put(str2buf(Long.toString(val)));
		buf.put((byte) terminator);
	}
}
