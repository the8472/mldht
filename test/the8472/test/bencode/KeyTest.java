package the8472.test.bencode;

import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.junit.Test;

import lbms.plugins.mldht.kad.Key;

public class KeyTest {

	@Test
	public void test() {
		assertEquals(Key.MIN_KEY.compareTo(Key.MAX_KEY), -1);
		assertEquals(Key.MAX_KEY.compareTo(Key.MIN_KEY), 1);
		assertEquals(Key.MAX_KEY.compareTo(Key.MAX_KEY), 0);
	}
	
	@Test
	public void testHashCode() {
		
		int hammingWeights[] = IntStream.range(0, 256).map(i -> Integer.bitCount(Key.createRandomKey().hashCode())).sorted().toArray();
		int median = hammingWeights[hammingWeights.length/2];
		
		assertEquals(median, 16);
		
	}

}
