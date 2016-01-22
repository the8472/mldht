package lbms.plugins.mldht.kad.messages;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;

public class GetRequest extends AbstractLookupRequest {
	
	long onlySendValueIfSeqGreaterThan = -1;

	public GetRequest(Key target) {
		super(target, Method.GET);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected String targetBencodingName() {
		return "target";
	}
	
	public void setSeq(long l) {
		onlySendValueIfSeqGreaterThan = l;
	}
	
	public long getSeq() {
		return onlySendValueIfSeqGreaterThan;
	}
	
	@Override
	public void apply(DHT dh_table) {
		dh_table.get(this);
	}

}
