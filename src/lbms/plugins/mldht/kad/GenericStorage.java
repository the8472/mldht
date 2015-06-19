package lbms.plugins.mldht.kad;

import static the8472.bencode.Utils.buf2ary;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import lbms.plugins.mldht.kad.messages.PutRequest;
import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;
import the8472.bencode.BEncoder;

public class GenericStorage {
	
	public static final long EXPIRATION_INTERVAL_SECONDS = 2*60*60;
	
	public static class StorageItem {
		
		public StorageItem(PutRequest req) {
			expirationDate = System.currentTimeMillis() + EXPIRATION_INTERVAL_SECONDS*1000;
			value = buf2ary(req.rawValue());
			
			if(req.getPubkey() != null) {
				sequenceNumber = req.getSequenceNumber();
				signature = req.getSignature();
				salt = req.getSalt();
				pubkey = req.getPubkey();
			} else {
				pubkey = null;
				salt = null;
			}
		}
		
		long expirationDate;
		long sequenceNumber = -1;
		byte[] signature;
		final byte[] pubkey;
		final byte[] salt;
		byte[] value;
		
		
		public boolean mutable() {
			return pubkey != null;
		}
		
		static final EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("ed25519-sha-512");
		
		public boolean validateSig()  {
			try {
				Signature sig = new EdDSAEngine();
				sig.initVerify(new EdDSAPublicKey(new EdDSAPublicKeySpec(pubkey, spec)));
				
				// ("4:salt" length-of-salt ":" salt) "3:seqi" seq "e1:v" len ":" and the encoded value
				
				Map<String, Object> p = new TreeMap<>();
				
				if(salt != null)
					p.put("salt", salt);
				p.put("seq", sequenceNumber);
				p.put("v", new BEncoder.RawData(ByteBuffer.wrap(value)));
				
				ByteBuffer buf = new BEncoder().encode(p, 1500);
				
				// trim d ... e
				buf.position(buf.position() + 1);
				buf.limit(buf.limit() - 1);
				
				sig.update(buf);
				
				return sig.verify(signature);
			} catch (InvalidKeyException | SignatureException e) {
				return false;
			}

		}
		
		public ByteBuffer getRawValue() {
			return ByteBuffer.wrap(value).asReadOnlyBuffer();
		}
		
	}
	
	ConcurrentHashMap<Key, StorageItem> items = new ConcurrentHashMap<>();
	
	
	enum UpdateResult {
		SUCCESS,
		IMMUTABLE_SUBSTITUTION_FAIL,
		SIG_FAIL,
		CAS_FAIL,
		SEQ_FAIL;
	}
	
	
	public UpdateResult putOrUpdate(Key k, StorageItem newItem, long expected) {
		
		if(newItem.mutable() && !newItem.validateSig())
			return UpdateResult.SIG_FAIL;
		
		while(true) {
			StorageItem oldItem = items.putIfAbsent(k, newItem);
			
			if(oldItem == null)
				return UpdateResult.SUCCESS;
			
			if(oldItem.mutable()) {
				if(!newItem.mutable())
					return UpdateResult.IMMUTABLE_SUBSTITUTION_FAIL;
				if(newItem.sequenceNumber < oldItem.sequenceNumber)
					return UpdateResult.SEQ_FAIL;
				if(expected >= 0 && oldItem.sequenceNumber >= 0 && oldItem.sequenceNumber != expected)
					return UpdateResult.CAS_FAIL;
			}
			
			if(items.replace(k, oldItem, newItem))
				break;
		}
		
		return UpdateResult.SUCCESS;
	}
	
	public Optional<StorageItem> get(Key k) {
		return Optional.ofNullable(items.get(k));
	}
	
	
	public void cleanup() {
		long now = System.currentTimeMillis();
		
		items.entrySet().removeIf(entry -> {
			return entry.getValue().expirationDate < now;
		});
	}
	
	public Map<Key, StorageItem> getItems() {
		return Collections.unmodifiableMap(items);
	}

}
