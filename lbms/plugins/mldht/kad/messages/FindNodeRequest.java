package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;

/**
 * @author Damokles
 *
 */
public class FindNodeRequest extends AbstractLookupRequest {

	/**
	 * @param id
	 * @param target
	 */
	public FindNodeRequest (Key target) {
		super(target,Method.FIND_NODE);
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.messages.MessageBase#apply(lbms.plugins.mldht.kad.DHT)
	 */
	@Override
	public void apply (DHT dh_table) {
		dh_table.findNode(this);
	}

	@Override
	protected String targetBencodingName() { return "target"; }
}
