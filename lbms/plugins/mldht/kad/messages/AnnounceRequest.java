package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;

import org.gudy.azureus2.core3.util.BEncoder;

/**
 * @author Damokles
 *
 */
public class AnnounceRequest extends GetPeersRequest {

	protected int		port;
	boolean				isSeed;
	protected byte[]	token;

	/**
	 * @param id
	 * @param info_hash
	 * @param port
	 * @param token
	 */
	public AnnounceRequest (Key id, Key info_hash, int port, byte[] token) {
		super(id, info_hash);
		this.port = port;
		this.token = token;
		this.method = Method.ANNOUNCE_PEER;
	}

	public boolean isSeed() {
		return isSeed;
	}

	public void setSeed(boolean isSeed) {
		this.isSeed = isSeed;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.messages.GetPeersRequest#apply(lbms.plugins.mldht.kad.DHT)
	 */
	@Override
	public void apply (DHT dh_table) {
		dh_table.announce(this);
	}
	
	
	@Override
	public Map<String, Object> getInnerMap() {
		Map<String, Object> inner = new HashMap<String, Object>();

		inner.put("id", id.getHash());
		inner.put("info_hash", target.getHash());
		inner.put("port", port);
		inner.put("token", token);
		inner.put("seed", Long.valueOf(isSeed ? 1 : 0));

		return inner;
	}


	/**
	 * @return the token
	 */
	public byte[] getToken () {
		return token;
	}
}
