package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;

import org.gudy.azureus2.core3.util.BEncoder;

/**
 * @author Damokles
 *
 */
public abstract class AbstractLookupRequest extends MessageBase {

	protected Key	target;
	private boolean want4;
	private boolean want6;

	/**
	 * @param id
	 * @param info_hash
	 */
	public AbstractLookupRequest (Key id, Key target, Method m) {
		super(new byte[] {(byte) 0xFF}, m, Type.REQ_MSG, id);
		this.target = target;
	}
	
	@Override
	public Map<String, Object> getInnerMap() {
		Map<String, Object> inner = new HashMap<String, Object>();
		inner.put("id", id.getHash());
		inner.put(targetBencodingName(), target.getHash());
		List<String> want = new ArrayList<String>(2);
		if(want4)
			want.add("n4");
		if(want6)
			want.add("n6");
		inner.put("want",want);

		return inner;

	}

	protected abstract String targetBencodingName();

	/**
	 * @return the info_hash
	 */
	public Key getTarget () {
		return target;
	}

	public boolean doesWant4() {
		return want4;
	}
	
	public void decodeWant(List<byte[]> want) {
		if(want == null)
			return;
		
		List<String> wants = new ArrayList<String>(2);
		for(byte[] bytes : want)
			wants.add(new String(bytes));
		
		want4 |= wants.contains("n4");
		want6 |= wants.contains("n6");
	}
	
	public void setWant4(boolean want4) {
		this.want4 = want4;
	}

	public boolean doesWant6() {
		return want6;
	}

	public void setWant6(boolean want6) {
		this.want6 = want6;
	}
	
	public String toString() {
		//return super.toString() + "targetKey:"+target+" ("+(160-DHT.getSingleton().getOurID().findApproxKeyDistance(target))+")";
		return super.toString() + "targetKey:"+target;
	}
}
