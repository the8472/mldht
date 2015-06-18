package the8472.bencode;

import static the8472.bencode.Utils.buf2ary;
import static the8472.bencode.Utils.buf2str;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BDecoder {
	
	enum ParserState {
		DICT_KEY,
		DICT_VALUE,
		LIST_VALUE,
		TREE_ROOT
	}
	
	private class TokenConsumer implements the8472.bencode.Tokenizer.TokenConsumer {
		Object[] stack = new Object[256];
		int nesting = 0;
		ParserState state = ParserState.TREE_ROOT;
		String currentKey = null;
		Map<String, Object> currentMap = null;
		List<Object> currentList = null;
		
		private void putObject(Object value) {
			switch(state) {
				case LIST_VALUE:
					currentList.add(value);
					break;
				case DICT_VALUE:
					assert(currentKey != null);
					currentMap.put(currentKey, value);
					currentKey = null;
					state = ParserState.DICT_KEY;
					break;
				case TREE_ROOT:
					break;
				case DICT_KEY:
					throw new Tokenizer.BDecodingException("encountered non-string type while decoding a dictionary key");
			}
		}

		
		public void dictionaryEnter() {
			Map newMap = new HashMap();
			putObject(newMap);
			stack[nesting] = currentMap = newMap;
			state = ParserState.DICT_KEY;
			nesting++;
		}
		
		public void listEnter() {
			List newList = new ArrayList();
			putObject(newList);
			stack[nesting] = currentList = newList;
			state = ParserState.LIST_VALUE;
			nesting++;
		}
		
		public void number(long num) {
			putObject(num);
		}
		
		public void string(ByteBuffer key) {
			if(state == ParserState.DICT_KEY) {
				currentKey = buf2str(key);
				state = ParserState.DICT_VALUE;
			} else {
				putObject(buf2ary(key));
			}
		}
		
		
		@SuppressWarnings("unchecked")
		public void nestingExit() {
			currentKey = null;
			currentList = null;
			currentMap = null;
			nesting--;
			if(nesting >= 1) {
				Object current = stack[nesting-1];
				if(current instanceof Map) {
					currentMap = (Map<String, Object>) current;
					state = ParserState.DICT_KEY;
				} else if(current instanceof List) {
					currentList = (List<Object>) current;
					state = ParserState.LIST_VALUE;
				}
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
		TokenConsumer consumer = new TokenConsumer();
		
		Tokenizer t = new Tokenizer();
		t.tokenize(buf, consumer);
		return consumer.stack[0];
	}


}
