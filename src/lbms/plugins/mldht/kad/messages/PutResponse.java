package lbms.plugins.mldht.kad.messages;

import lbms.plugins.mldht.kad.DHT;

public class PutResponse extends MessageBase {

	public PutResponse(byte[] mtid) {
		super(mtid, Method.PUT, Type.RSP_MSG);
	}
	
	@Override
	public void apply(DHT dh_table) {
		dh_table.response(this);
	}

}
