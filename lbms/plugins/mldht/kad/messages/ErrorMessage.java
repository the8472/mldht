package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;

import org.gudy.azureus2.core3.util.BEncoder;

/**
 * @author Damokles
 *
 */
public class ErrorMessage extends MessageBase {

	private String	msg;
	private int		code;

	/**
	 * @param mtid
	 * @param id
	 * @param msg
	 */
	public ErrorMessage (byte[] mtid, int code, String msg) {
		super(mtid, Method.NONE, Type.ERR_MSG, null);
		this.msg = msg;
		this.code = code;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.messages.MessageBase#apply(lbms.plugins.mldht.kad.DHT)
	 */
	@Override
	public void apply (DHT dh_table) {
		dh_table.error(this);
	}
	
	@Override
	public Map<String, Object> getBase() {
		Map<String, Object> base = super.getBase();
		List<Object> errorDetails = new ArrayList<Object>(2);
		errorDetails.add(code);
		errorDetails.add(msg);
		base.put(getType().innerKey(), errorDetails);
		
		return base;
	}



	/**
	 * @return the Message
	 */
	public String getMessage () {
		return msg;
	}

	/**
	 * @return the code
	 */
	public int getCode () {
		return code;
	}

	public static enum ErrorCode {
		GenericError(201),
		ServerError(202),
		ProtocolError(203), //such as a malformed packet, invalid arguments, or bad token
		MethodUnknown(204);

		public final int code;

        private ErrorCode(int code) {
        	this.code = code;
        }
	}
	
	@Override
	public String toString() {
		return super.toString() + " code:"+ code + " errormsg: '"+msg+"'";
	}
}
