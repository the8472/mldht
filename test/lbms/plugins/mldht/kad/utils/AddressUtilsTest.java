package lbms.plugins.mldht.kad.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

public class AddressUtilsTest {
	
	@Test
	public void testGlobalUnicastMatcher() throws UnknownHostException {
		assertTrue(AddressUtils.isGlobalUnicast(InetAddress.getByName("8.8.8.8")));
		assertTrue(AddressUtils.isGlobalUnicast(InetAddress.getByName("2001:4860:4860::8888")));
		// wildcard
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("0.0.0.0")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("0.150.0.0")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("::0")));
		// loopback
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("127.0.0.15")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("::1")));
		// private/LL
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("192.168.13.47")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("169.254.1.0")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("fe80::")));
		// ULA
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("fc00::")));
		assertFalse(AddressUtils.isGlobalUnicast(InetAddress.getByName("fd00::")));
	}

}
