package the8472.bencode;

import static the8472.bencode.Utils.buf2ary;
import static the8472.bencode.Utils.buf2str;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import the8472.bencode.Tokenizer.BDecodingException;
import the8472.bencode.Tokenizer.Token;
import the8472.bencode.Tokenizer.TokenConsumer;

public class BDecoder {
	
	enum ParserState {
		DICT_KEY,
		DICT_VALUE,
		LIST_VALUE,
		TREE_ROOT
	}
	
	private class Consumer implements TokenConsumer {
		
		Tokenizer t;
		
		Object[] stack = new Object[256];
		
		String keyPendingInsert;
		
		int depth = 0;
		
		@Override
		public void push(Token st) {
			
			//System.out.println("push"+st.type());
			
			switch(st.type()) {
				case DICT:
					Object o = new HashMap<String, Object>();
					putObject(o);
					pushInternal(o);
					break;
				case LIST:
					o = new ArrayList<>();
					putObject(o);
					pushInternal(o);
					break;
				case LONG:
				case STRING:
				case PREFIXED_STRING:
					return;
				default:
					throw new IllegalStateException("this shouldn't be happening");
			}
			
			
			
			depth++;
		}
		
		void pushInternal(Object o) {
			stack[depth] = o;
		}
		
		void putObject(Object o) {
			if(depth == 0) {
				stack[0] = o;
				return;
			}
			
			Object container = stack[depth - 1];
			
			
			if(container.getClass() == HashMap.class) {
				if(keyPendingInsert != null) {
					if(o instanceof ByteBuffer)
						o = buf2ary((ByteBuffer)o);
					if(((HashMap<String, Object>)container).put(keyPendingInsert, o) != null)
						throw new BDecodingException("duplicate key found in dictionary");
					keyPendingInsert = null;
					
				} else {
					keyPendingInsert = buf2str((ByteBuffer)o);
				}
			} else if(container.getClass() == ArrayList.class) {
				if(o instanceof ByteBuffer)
					o = buf2ary((ByteBuffer)o);
				((ArrayList<Object>)container).add(o);
			} else {
				throw new RuntimeException("this should not happen");
			}

		}

		@Override
		public void pop(Token st) {
			//System.out.println("pop"+st.type());
			
			switch(st.type()) {
				case DICT:
				case LIST:
					depth--;
					return;
				case LONG:
					putObject(t.lastDecodedNum);
					break;
				case STRING:
					putObject(t.getSlice(st));
					break;
				case PREFIXED_STRING:
					return;
				default:
					throw new IllegalStateException("this shouldn't be happening");
			}
			
		}
		

	}
	
	
	public Map<String, Object> decode(ByteBuffer buf) {
		Object root = decodeInternal(buf);
		if(root instanceof Map) {
			return (Map<String, Object>) root;
		}
		throw new RuntimeException("expected dictionary as root object");
	}
	
	private Object decodeInternal(ByteBuffer buf) {
		Consumer consumer = new Consumer();
		
		
		Tokenizer t = new Tokenizer();
		consumer.t = t;
		t.inputBuffer(buf);
		t.consumer(consumer);
		t.tokenize();
		return consumer.stack[0];
	}


}
