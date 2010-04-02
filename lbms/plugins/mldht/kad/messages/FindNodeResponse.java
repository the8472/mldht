package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTConstants;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.DHTtype;

/**
 * @author Damokles
 *
 */
public class FindNodeResponse extends MessageBase {

	protected byte[]	nodes;
	protected byte[]	nodes6;

	/**
	 * @param mtid
	 * @param id
	 * @param nodes
	 */
	public FindNodeResponse (byte[] mtid, byte[] nodes, byte[] nodes6) {
		super(mtid, Method.FIND_NODE, Type.RSP_MSG);
		this.nodes = nodes;
		this.nodes6 = nodes6;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.messages.MessageBase#apply(lbms.plugins.mldht.kad.DHT)
	 */
	@Override
	public void apply (DHT dh_table) {
		dh_table.response(this);
	}
	
	@Override
	public Map<String, Object> getInnerMap() {
		Map<String, Object> inner = new HashMap<String, Object>();
		inner.put("id", id.getHash());
		if(nodes != null)
			inner.put("nodes", nodes);
		if(nodes6 != null)
			inner.put("nodes6", nodes6);
		

		return inner;		
	}
	
	public byte[] getNodes(DHTtype type)
	{
		if(type == DHTtype.IPV4_DHT)
			return nodes;
		if(type == DHTtype.IPV6_DHT)
			return nodes6;
		return null;
	}

	/**
	 * @return the nodes
	 */
	public byte[] getNodes () {
		return nodes;
	}
	
	/**
	 * @return the nodes
	 */
	public byte[] getNodes6 () {
		return nodes6;
	}
	
	public String toString() {
		return super.toString() + (nodes != null ? "contains: "+ (nodes.length/DHTtype.IPV4_DHT.NODES_ENTRY_LENGTH) + " nodes" : "") + (nodes6 != null ? "contains: "+ (nodes6.length/DHTtype.IPV6_DHT.NODES_ENTRY_LENGTH) + " nodes6" : "");
	}
}
