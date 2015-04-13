/*
 *    This file is part of mlDHT.
 * 
 *    mlDHT is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 2 of the License, or
 *    (at your option) any later version.
 * 
 *    mlDHT is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 * 
 *    You should have received a copy of the GNU General Public License
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>.
 */
package lbms.plugins.mldht.kad.messages;

import static the8472.bencode.Utils.prettyPrint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lbms.plugins.mldht.kad.BloomFilterBEP33;
import lbms.plugins.mldht.kad.DBItem;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.ErrorMessage.ErrorCode;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.utils.AddressUtils;

/**
 * @author Damokles
 * 
 */
public class MessageDecoder {

	public static MessageBase parseMessage (Map<String, Object> map,
			RPCServer srv) throws MessageException, IOException {

		try {
			String msgType = getStringFromBytes((byte[]) map.get(Type.TYPE_KEY), true);
			if (msgType == null) {
				throw new MessageException("message type (y) missing", ErrorCode.ProtocolError);
			}

			String version = getStringFromBytes((byte[]) map.get(MessageBase.VERSION_KEY),true);

			MessageBase mb = null;
			if (msgType.equals(Type.REQ_MSG.getRPCTypeName())) {
				mb = parseRequest(map, srv);
			} else if (msgType.equals(Type.RSP_MSG.getRPCTypeName())) {
				mb = parseResponse(map, srv);
			} else if (msgType.equals(Type.ERR_MSG.getRPCTypeName())) {
				mb = parseError(map, srv);
			} else
				throw new MessageException("unknown RPC type (y="+msgType+")");

			if (mb != null && version != null) {
				mb.setVersion(version);
			}

			return mb;
		} catch (Exception e) {
			if(e instanceof MessageException)
				throw (MessageException)e;
			throw new IOException("could not parse message",e);
		}
	}

	/**
	 * @param map
	 * @return
	 */
	private static MessageBase parseError (Map<String, Object> map, RPCServer srv) {
		Object error = map.get(Type.ERR_MSG.innerKey());
		
		int errorCode = 0;
		String errorMsg = null;
		
		if(error instanceof byte[])
			errorMsg = getStringFromBytes((byte[])error);
		else if (error instanceof List<?>)
		{
			List<Object> errmap = (List<Object>)error;
			try
			{
				errorCode = ((Long) errmap.get(0)).intValue();
				errorMsg = getStringFromBytes((byte[]) errmap.get(1));
			} catch (Exception e)
			{
				// do nothing
			}
		}
		
		Object rawMtid = map.get(MessageBase.TRANSACTION_KEY);
		
		if (errorMsg == null && (rawMtid == null || !(rawMtid instanceof byte[])))
			return null;

		byte[] mtid = (byte[]) rawMtid;
		
		ErrorMessage msg = new ErrorMessage(mtid, errorCode,errorMsg);
		
		RPCCall call = srv.findCall(mtid);
		if(call != null)
			msg.method = call.getMessageMethod();

		return msg;
	}

	/**
	 * @param map
	 * @param srv
	 * @return
	 */
	private static MessageBase parseResponse (Map<String, Object> map,RPCServer srv) throws MessageException {

		byte[] mtid = (byte[]) map.get(MessageBase.TRANSACTION_KEY);
		if (mtid == null || mtid.length < 1)
			throw new MessageException("missing transaction ID",ErrorCode.ProtocolError);
		
		// responses don't have explicit methods, need to match them to a request to figure that one out
		Method m = Optional.ofNullable(srv.findCall(mtid)).map(c -> c.getMessageMethod()).orElse(Method.UNKNOWN);

		return parseResponse(map, m, mtid);
	}

	/**
	 * @param map
	 * @param msgMethod
	 * @param mtid
	 * @return
	 */
	private static MessageBase parseResponse (Map<String, Object> map,	Method msgMethod, byte[] mtid) throws MessageException {
		Map<String, Object> args = (Map<String, Object>) map.get(Type.RSP_MSG.innerKey());
		if (args == null) {
			throw new MessageException("response did not contain a body",ErrorCode.ProtocolError);
		}

		byte[] hash = (byte[]) args.get("id");
		byte[] ip = (byte[]) map.get(MessageBase.EXTERNAL_IP_KEY);

		if (hash == null || hash.length != Key.SHA1_HASH_LENGTH) {
			throw new MessageException("invalid or missing origin ID",ErrorCode.ProtocolError);
		}

		Key id = new Key(hash);
		
		MessageBase msg = null;

		switch (msgMethod) {
		case PING:
			msg = new PingResponse(mtid);
			break;
		case ANNOUNCE_PEER:
			msg = new AnnounceResponse(mtid);
			break;
		case FIND_NODE:
			if (!args.containsKey("nodes") && !args.containsKey("nodes6"))
				throw new MessageException("received response to find_node request with neither 'nodes' nor 'nodes6' entry", ErrorCode.ProtocolError);
				//return null;
			
			msg = new FindNodeResponse(mtid, (byte[]) args.get("nodes"),(byte[])args.get("nodes6"));
			break;
		case GET_PEERS:
			byte[] token = (byte[]) args.get("token");
			byte[] nodes = (byte[]) args.get("nodes");
			byte[] nodes6 = (byte[]) args.get("nodes6");

			
			List<DBItem> dbl = null;
			Object rawVals = args.get("values");
			if(rawVals != null)
			{
				if(!(rawVals instanceof List))
					throw new MessageException("values field in get_peers response was a string, expected a list of strings", ErrorCode.ProtocolError);
				List<byte[]> vals = (List<byte[]>) rawVals;
				if(vals.size() > 0)
				{
					dbl = new ArrayList<DBItem>(vals.size());
					for (int i = 0; i < vals.size(); i++)
					{
						// only accept ipv4 or ipv6 for now
						if (vals.get(i).length != DHTtype.IPV4_DHT.ADDRESS_ENTRY_LENGTH && vals.get(i).length != DHTtype.IPV6_DHT.ADDRESS_ENTRY_LENGTH)
							continue;
						dbl.add(new PeerAddressDBItem(vals.get(i), false));
					}
				}
			}
			
			byte[] peerFilter = (byte[]) args.get("BFpe");
			byte[] seedFilter = (byte[]) args.get("BFse");
			
			if((peerFilter != null && peerFilter.length != BloomFilterBEP33.m/8) || (seedFilter != null && seedFilter.length != BloomFilterBEP33.m/8))
				throw new MessageException("invalid BEP33 filter length", ErrorCode.ProtocolError);
			
			if (dbl != null || nodes != null || nodes6 != null)
			{
				GetPeersResponse resp = new GetPeersResponse(mtid, nodes, nodes6, token);
				resp.setPeerItems(dbl);
				resp.setScrapePeers(peerFilter);
				resp.setScrapeSeeds(seedFilter);
				msg = resp;
				break;
			}
			
			throw new MessageException("Neither nodes nor values in get_peers response",ErrorCode.ProtocolError);
		case UNKNOWN:
			msg = new UnknownTypeResponse(mtid, null, null);
			break;
 		default:
			throw new RuntimeException("should not happen!!!");
		}
		
		if(ip != null) {
			InetSocketAddress addr = AddressUtils.unpackAddress(ip);
			msg.setPublicIP(addr);
			if(addr == null)
				DHT.logError("could not decode IP: " + prettyPrint(map));
		}
		
		msg.setID(id);
		
		return msg;
	}

	/**
	 * @param map
	 * @return
	 */
	private static MessageBase parseRequest (Map<String, Object> map, RPCServer srv) throws MessageException {
		Object rawRequestMethod = map.get(Type.REQ_MSG.getRPCTypeName());
		Map<String, Object> args = (Map<String, Object>) map.get(Type.REQ_MSG.innerKey());
		
		if (rawRequestMethod == null || args == null)
			return null;

		byte[] mtid = (byte[])map.get(MessageBase.TRANSACTION_KEY);
		byte[] hash = (byte[]) args.get("id");
		
		if(hash == null || hash.length != Key.SHA1_HASH_LENGTH)
			throw new MessageException("missing or invalid node ID", ErrorCode.ProtocolError);
		if(mtid == null || mtid.length < 1)
			throw new MessageException("missing or zero-length transaction ID in response", ErrorCode.ProtocolError);

		Key id = new Key(hash);

		MessageBase msg = null;

		String requestMethod = getStringFromBytes((byte[]) rawRequestMethod);
		if (Method.PING.getRPCName().equals(requestMethod)) {
			msg = new PingRequest();
		} else if (Method.FIND_NODE.getRPCName().equals(requestMethod) || Method.GET_PEERS.getRPCName().equals(requestMethod)) {
			hash = (byte[]) args.get("target");
			if (hash == null)
				hash = (byte[]) args.get("info_hash");
			if (hash == null || hash.length != Key.SHA1_HASH_LENGTH)
				throw new MessageException("missing/invalid hash in request",ErrorCode.ProtocolError);
			AbstractLookupRequest req = Method.FIND_NODE.getRPCName().equals(requestMethod) ? new FindNodeRequest(new Key(hash)) : new GetPeersRequest(new Key(hash));
			
			List<byte[]> explicitWants = (List<byte[]>) args.get("want");
					
			if(explicitWants != null)
				req.decodeWant(explicitWants);
			else {
				req.setWant4(srv.getDHT().getType() == DHTtype.IPV4_DHT);
				req.setWant6(srv.getDHT().getType() == DHTtype.IPV6_DHT);
			}
			
			
			if (req instanceof GetPeersRequest)
			{
				GetPeersRequest peerReq = (GetPeersRequest) req;
				peerReq.setNoSeeds(Long.valueOf(1).equals(args.get("noseed")));
				peerReq.setScrape(Long.valueOf(1).equals(args.get("scrape")));
			}
			msg = req;
		} else if (Method.ANNOUNCE_PEER.getRPCName().equals(requestMethod)) {
			if (args.containsKey("info_hash") && args.containsKey("port") && args.containsKey("token")) {
				hash = (byte[]) args.get("info_hash");
				if (hash == null || hash.length != Key.SHA1_HASH_LENGTH)
					throw new MessageException("invalid hash in request",ErrorCode.ProtocolError);
				Key infoHash = new Key(hash);

				byte[] token = (byte[]) args.get("token");
				if(token.length == 0)
					throw new MessageException("zero-length token in announce_peer request. see BEP33 for reasons why tokens might not have been issued by get_peers response", ErrorCode.ProtocolError);
				
				AnnounceRequest ann = new AnnounceRequest(infoHash, ((Long) args.get("port")).intValue(), token);
				ann.setSeed(Long.valueOf(1).equals(args.get("seed")));

				msg = ann;
			} else {
				throw new MessageException("missing arguments for announce",ErrorCode.ProtocolError);
			}
				
		} else {
			// we don't know what request type this is. check for a target being present
			Object target = args.get("info_hash");
			if(target == null || !(target instanceof byte[]))
				target = args.get("target");
			if(target != null && target instanceof byte[] && ((byte[])target).length == Key.SHA1_HASH_LENGTH)
			{
				AbstractLookupRequest req = new UnknownTypeRequest(new Key((byte[])target));
				
				List<byte[]> explicitWants = (List<byte[]>) args.get("want");
				
				if(explicitWants != null)
					req.decodeWant(explicitWants);
				else {
					req.setWant4(srv.getDHT().getType() == DHTtype.IPV4_DHT);
					req.setWant6(srv.getDHT().getType() == DHTtype.IPV6_DHT);
				}
				
				msg = req;
			} else
			{
				throw new MessageException("Received unknown Message Type: " + requestMethod,ErrorCode.MethodUnknown);
			}
			
		}

		if (msg != null) {
			msg.setMTID(mtid);
			msg.setID(id);
		}

		return msg;
	}
	
	private static String getStringFromBytes (byte[] bytes, boolean preserveBytes) {
		if (bytes == null) {
			return null;
		}
		try {
			return new String(bytes, preserveBytes ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
		} catch (Exception e) {
			DHT.log(e, LogLevel.Verbose);
			return null;
		}
	}

	private static String getStringFromBytes (byte[] bytes) {
		return getStringFromBytes(bytes, false);
	}
}
