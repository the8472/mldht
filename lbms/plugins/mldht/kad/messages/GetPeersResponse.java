package lbms.plugins.mldht.kad.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lbms.plugins.mldht.kad.*;
import lbms.plugins.mldht.kad.DHT.DHTtype;

import org.gudy.azureus2.core3.util.BEncoder;

/**
 * @author Damokles
 *
 */
public class GetPeersResponse extends MessageBase {

	private byte[]			token;
	private byte[]			nodes;
	private byte[]			nodes6;
	private byte[]			scrapeSeeds;
	private byte[]			scrapePeers;

	private List<DBItem>	items;

	/**
	 * @param mtid
	 * @param id
	 * @param nodes
	 * @param token
	 */
	public GetPeersResponse (byte[] mtid, Key id, byte[] nodes, byte[] nodes6, byte[] token) {
		super(mtid, Method.GET_PEERS, Type.RSP_MSG, id);
		this.nodes = nodes;
		this.nodes6 = nodes6;
		this.token = token;
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
		Map<String, Object> innerMap = new HashMap<String, Object>();
		innerMap.put("id", id.getHash());
		if(token != null)
			innerMap.put("token", token);
		if(nodes != null)
			innerMap.put("nodes", nodes);
		if(nodes6 != null)
			innerMap.put("nodes6", nodes6);
		if(items != null && !items.isEmpty()) {
			List<byte[]> itemsList = new ArrayList<byte[]>(items.size());
			for (DBItem item : items) {
				itemsList.add(item.getData());
			}
			innerMap.put("values", itemsList);
		}

		if(scrapePeers != null && scrapeSeeds != null)
		{
			innerMap.put("BFpe", scrapePeers);
			innerMap.put("BFse", scrapeSeeds);
		}

		return innerMap;
	}

	public byte[] getNodes(DHTtype type)
	{
		if(type == DHTtype.IPV4_DHT)
			return nodes;
		if(type == DHTtype.IPV6_DHT)
			return nodes6;
		return null;
	}
	
	public void setPeerItems(List<DBItem> items) {
		this.items = items;
	}

	public List<DBItem> getPeerItems () {
		return items == null ? (List<DBItem>)Collections.EMPTY_LIST : Collections.unmodifiableList(items);
	}
	
	public BloomFilter getScrapeSeeds() {
		if(scrapeSeeds != null)
			return new BloomFilter(scrapeSeeds);
		return null;
	}


	public void setScrapeSeeds(BloomFilter scrapeSeeds) {
		this.scrapeSeeds = scrapeSeeds.serialize();
	}


	public BloomFilter getScrapePeers() {
		if(scrapePeers != null)
			return new BloomFilter(scrapePeers);
		return null;
	}


	public void setScrapePeers(BloomFilter scrapePeers) {
		this.scrapePeers = scrapePeers.serialize();
	}

	public byte[] getToken () {
		return token;
	}

	
	public String toString() {
		return super.toString() + "contains: "+ (nodes != null ? (nodes.length/DHTtype.IPV4_DHT.NODES_ENTRY_LENGTH)+" nodes" : "") + (nodes6 != null ? (nodes6.length/DHTtype.IPV6_DHT.NODES_ENTRY_LENGTH)+" nodes6" : "") + (items != null ? (items.size())+" values" : "") ;
	}
}
