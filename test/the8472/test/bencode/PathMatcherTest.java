package the8472.test.bencode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static the8472.bencode.Utils.str2buf;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import the8472.bencode.PathMatcher;
import the8472.bencode.Tokenizer;

public class PathMatcherTest {
	
	
	
	@Test
	public void testMatcher() throws InterruptedException, ExecutionException {
		
		ByteBuffer buf = str2buf("d3:food3:barleee");
		
		PathMatcher m = new PathMatcher("foo", "bar");
		
		Tokenizer t = new Tokenizer();

		m.tokenizer(t);
		ByteBuffer result = m.match(buf);
		
		assertNotNull(result);
		assertEquals(str2buf("le"), result);
				
	}

}
