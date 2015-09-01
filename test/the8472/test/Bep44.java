package the8472.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static the8472.bencode.Utils.str2ary;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Test;

import lbms.plugins.mldht.kad.GenericStorage.StorageItem;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.messages.PutRequest;

public class Bep44 {
	
	@Test
	public void testMutable() {
		byte[] value =  str2ary("12:Hello World!");
		byte[] pubkey =  new BigInteger("77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548", 16).toByteArray();
		byte[] sig = new BigInteger("305ac8aeb6c9c151fa120f120ea2cfb923564e11552d06a5d856091e5e853cff1260d3f39e4999684aa92eb73ffd136e6f4f3ecbfda0ce53a1608ecd7ae21f01", 16).toByteArray();
		Key target = new Key("4a533d47ec9c7d95b1ad75f576cffc641853b750");
		
		PutRequest req = new PutRequest();
		
		req.setPubkey(pubkey);
		req.setValue(ByteBuffer.wrap(value));
		req.setSignature(sig);
		req.setSequenceNumber(1);
		
		assertEquals(req.deriveTargetKey(), target);
		
		StorageItem item = new StorageItem(req);
		
		assertTrue(item.validateSig());
	}

	@Test
	public void testSaltedMutable() {
		byte[] value =  str2ary("12:Hello World!");
		byte[] salt = str2ary("foobar");
		byte[] pubkey =  new BigInteger("77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548", 16).toByteArray();
		byte[] sig = new BigInteger("6834284b6b24c3204eb2fea824d82f88883a3d95e8b4a21b8c0ded553d17d17ddf9a8a7104b1258f30bed3787e6cb896fca78c58f8e03b5f18f14951a87d9a08", 16).toByteArray();
		Key target = new Key("411eba73b6f087ca51a3795d9c8c938d365e32c1");
		
		PutRequest req = new PutRequest();
		
		req.setPubkey(pubkey);
		req.setValue(ByteBuffer.wrap(value));
		req.setSalt(salt);
		req.setSignature(sig);
		req.setSequenceNumber(1);
		
		assertEquals(req.deriveTargetKey(), target);
		
		StorageItem item = new StorageItem(req);
		
		assertTrue(item.validateSig());
	}
	
	@Test
	public void testImmutable() {
		byte[] value =  str2ary("12:Hello World!");
		Key target = new Key("e5f96f6f38320f0f33959cb4d3d656452117aadb");
		
		PutRequest req = new PutRequest();
		
		req.setValue(ByteBuffer.wrap(value));
		
		assertEquals(req.deriveTargetKey(), target);
	}
	
	
}
