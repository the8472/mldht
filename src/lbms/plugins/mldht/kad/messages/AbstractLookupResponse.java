package lbms.plugins.mldht.kad.messages;

import java.util.Map;
import java.util.TreeMap;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;

public class AbstractLookupResponse extends MessageBase {
	
	protected byte[]	nodes;
	protected byte[]	nodes6;
	private byte[]			token;

	public void setNodes(byte[] nodes) {
		this.nodes = nodes;
	}
	
	public void setNodes6(byte[] nodes6) {
		this.nodes6 = nodes6;
	}
	

	public byte[] getToken () {
		return token;
	}
	
	public void setToken(byte[] t) {
		token = t;
	}
	
	@Override
	public void apply(DHT dh_table) {
		dh_table.response(this);
		
	}

	
	
	public AbstractLookupResponse(byte[] mtid, Method m, Type t) {
		super(mtid, m, t);
	}
	
	
	@Override
	public Map<String, Object> getInnerMap() {
		Map<String, Object> inner = new TreeMap<String, Object>();
		inner.put("id", id.getHash());
		if(token != null)
			inner.put("token", token);
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
	
	@Override
	public String toString() {
		return super.toString() + (nodes != null ? "contains: "+ (nodes.length/DHTtype.IPV4_DHT.NODES_ENTRY_LENGTH) + " nodes" : "") + (nodes6 != null ? "contains: "+ (nodes6.length/DHTtype.IPV6_DHT.NODES_ENTRY_LENGTH) + " nodes6" : "") +
				(token != null ? "token "+token.length+" | " : "");
	}
	

}
