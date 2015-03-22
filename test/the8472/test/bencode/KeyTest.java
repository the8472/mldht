package the8472.test.bencode;

import static org.junit.Assert.assertEquals;
import lbms.plugins.mldht.kad.Key;

import org.junit.Test;

public class KeyTest {

	@Test
	public void test() {
		assertEquals(Key.MIN_KEY.compareTo(Key.MAX_KEY), -1);
		assertEquals(Key.MAX_KEY.compareTo(Key.MIN_KEY), 1);
		assertEquals(Key.MAX_KEY.compareTo(Key.MAX_KEY), 0);
	}

}
