package the8472.bencode;

import java.nio.ByteBuffer;

public class Tokenizer {
	
	public static class BDecodingException extends RuntimeException {
		
		public BDecodingException(String msg) {
			super(msg);
		}
		
	}
	
	public static interface TokenConsumer {

		default void dictionaryEnter() {}

		default void nestingExit() {}

		default void string(ByteBuffer key) {}

		default void number(long num) {}

		default void listEnter() {}

		default void endOfRoot() {}
		
	}
	
	public void tokenize(ByteBuffer buf, TokenConsumer consumer) {
		int nesting = 0;
		
		parse: while(buf.remaining() > 0) {
			byte current = buf.get();
			
			switch(current) {
				case 'd':
					nesting++;
					consumer.dictionaryEnter();
					break;
				case 'i':
					long num = this.parseNum(buf, (byte) 'e');
					consumer.number(num);
					break;
				case 'l':
					nesting++;
					consumer.listEnter();
					break;
				case 'e':
					nesting--;
					consumer.nestingExit();
					if(nesting == 0)
						break parse;
					break;
				case '-':
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
				case '8':
				case '9':
					buf.position(buf.position()-1);
					long length = this.parseNum(buf, (byte) ':');
					if(length < 0)
						length = 0;
					ByteBuffer key = buf.slice();
					if(length > key.capacity())
						throw new BDecodingException("string (offset: "+buf.position()+" length: "+length+") points beyond end of message (length: "+buf.limit()+")");
					key.limit((int) length);
					consumer.string(key);
					buf.position((int) (buf.position() + length));
					break;
				default:
					StringBuilder b = new StringBuilder();
					Utils.toHex(new byte[]{current}, b , 1);
					throw new BDecodingException("unexpected character 0x" + b + " at offset "+(buf.position()-1));
					
			
			}
		}
		
		consumer.endOfRoot();
	}
	
	public long parseNum(ByteBuffer buf, byte terminator) {
		long result = 0;
		boolean neg = false;

		if(buf.remaining() < 1)
			throw new BDecodingException("end of message reached while decoding a number/string length prefix. offset:"+buf.position());
		byte current = buf.get();
		
		if(current == '-') {
			neg = true;
			if(buf.remaining() < 1)
				throw new BDecodingException("end of message reached while decoding a number/string length prefix. offset:"+buf.position());
			current = buf.get();
		}
		
		int iter = 0;
		
		while (current != terminator) {
			// do zero-check on 2nd character, since 0 itself is a valid length
			if(iter > 0 && result == 0)
				throw new BDecodingException("encountered a leading zero at offset "+(buf.position()-1)+" while decoding a number/string length prefix");
			
			if(current < '0' || current > '9') {
				StringBuilder b = new StringBuilder();
				Utils.toHex(new byte[]{current}, b , 1);
				throw new BDecodingException("encountered invalid character 0x"+b+" (offset:"+ (buf.position()-1) +") while decoding a number/string length prefix, expected 0-9 or "+ (char)terminator);
			}
				
			
			int digit = current - '0';
			
			result *= 10;
			result += digit;

			if(buf.remaining() < 1)
				throw new BDecodingException("end of message reached while decoding a number/string length prefix. offset:"+buf.position());
			current = buf.get();
			
			iter++;
		}
		
		if(neg)
			result *= -1;
		return result;
	}

}
