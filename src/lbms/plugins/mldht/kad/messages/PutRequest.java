package lbms.plugins.mldht.kad.messages;

import static the8472.bencode.Utils.buf2ary;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.messages.ErrorMessage.ErrorCode;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import the8472.bencode.BEncoder;

public class PutRequest extends MessageBase {
	
	/*
{
    "a":
    {
        "cas": <optional expected seq-nr (int)>,
        "id": <20 byte id of sending node (string)>,
        "k": <ed25519 public key (32 bytes string)>,
        "salt": <optional salt to be appended to "k" when hashing (string)>
        "seq": <monotonically increasing sequence number (integer)>,
        "sig": <ed25519 signature (64 bytes string)>,
        "token": <write-token (string)>,
        "v": <any bencoded type, whose encoded size < 1000>
    },
    "t": <transaction-id (string)>,
    "y": "q",
    "q": "put"
}
	 */
	
	long expectedSequenceNumber = -1;
	long sequenceNumber = -1;
	byte[] pubkey;
	byte[] salt;
	byte[] signature;
	byte[] token;
	// TODO extract raw bencoded data from incoming message
	Object value;
	
	

	public PutRequest() {
		super(null, Method.PUT, Type.REQ_MSG);
	}
	
	
	@Override
	public void apply(DHT dh_table) {
		
		dh_table.put(this);
	}
	
	public boolean mutable() {
		return pubkey != null;
	}
	
	public void validate() throws MessageException {
		if(salt != null && salt.length > 64)
			throw new MessageException("salt too long", ErrorCode.SaltTooBig);
		if(token == null || value == null)
			throw new MessageException("required arguments for PUT request missing", ErrorCode.ProtocolError);
		if(rawValue().limit() > 1000)
			throw new MessageException("bencoded PUT value ('v') field exceeds 1000 bytes", ErrorCode.PutMessageTooBig);
		if((pubkey != null || salt != null || signature != null || expectedSequenceNumber >= 0 || sequenceNumber >= 0) && (pubkey == null || signature == null))
			throw new MessageException("PUT request contained at least one field indicating mutable data but other fields mandatory for mutable PUTs were missing", ErrorCode.ProtocolError);
	}
	
	public byte[] getToken() {
		return token;
	}
	
	// TODO: extract raw value directly from incoming message instead of re-encoding
	public ByteBuffer rawValue() {
		return new BEncoder().encode(value, 2048);
	}
	
	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public byte[] getPubkey() {
		return pubkey;
	}

	public byte[] getSalt() {
		return salt;
	}

	public byte[] getSignature() {
		return signature;
	}
	
	public Key deriveTargetKey() {
		MessageDigest dig = ThreadLocalUtils.getThreadLocalSHA1();
		
		if(mutable()) {
			dig.reset();
			dig.update(pubkey);
			if(salt != null)
				dig.update(salt);
			return new Key(dig.digest());
		}
		return new Key(dig.digest(buf2ary(rawValue())));
	}




	public long getExpectedSequenceNumber() {
		return expectedSequenceNumber;
	}




	public void setExpectedSequenceNumber(long expectedSequenceNumber) {
		this.expectedSequenceNumber = expectedSequenceNumber;
	}




	public Object getValue() {
		return value;
	}




	public void setValue(Object value) {
		this.value = value;
	}




	public void setSequenceNumber(long sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}




	public void setPubkey(byte[] pubkey) {
		this.pubkey = pubkey;
	}




	public void setSalt(byte[] salt) {
		this.salt = salt;
	}




	public void setSignature(byte[] signature) {
		this.signature = signature;
	}




	public void setToken(byte[] token) {
		this.token = token;
	}

}
