package lbms.plugins.mldht.kad.messages;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;

public class UnknownTypeRequest extends AbstractLookupRequest {

	public UnknownTypeRequest(Key target) {
		super(target, Method.UNKNOWN);
	}
	
	protected String targetBencodingName() {
		return null;
	}
	
	public void apply(DHT dh_table) {
		dh_table.findNode(this);
	}
	
}
