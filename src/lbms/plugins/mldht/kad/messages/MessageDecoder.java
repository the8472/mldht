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
import static the8472.utils.Functional.castOrThrow;
import static the8472.utils.Functional.tap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

		byte[] hash = Optional.ofNullable(args.get("id"))
				.map(castOrThrow(byte[].class, (o) -> new MessageException("expected parameter 'id' to be a byte-string, got "+o.getClass().getSimpleName(), ErrorCode.ProtocolError)))
				.orElseThrow(() -> new MessageException("mandatory parameter 'id' missing", ErrorCode.ProtocolError));
		byte[] ip = (byte[]) map.get(MessageBase.EXTERNAL_IP_KEY);

		if (hash.length != Key.SHA1_HASH_LENGTH) {
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
			
			msg = tap(new FindNodeResponse(mtid), (m) -> {
				m.setNodes((byte[]) args.get("nodes"));
				m.setNodes6((byte[])args.get("nodes6"));
			});
			break;
		case GET_PEERS:
			byte[] token = typedGet(args, "token", byte[].class).orElse(null);
			byte[] nodes = typedGet(args, "nodes", byte[].class).orElse(null);
			byte[] nodes6 = typedGet(args, "nodes6", byte[].class).orElse(null);

			
			List<DBItem> dbl = null;
			
			@SuppressWarnings("unchecked")
			List<byte[]> vals = Optional.ofNullable(args.get("values"))
				.map(castOrThrow(List.class, val -> new MessageException("expected 'values' field in get_peers to be list of strings, got "+val.getClass(), ErrorCode.ProtocolError)))
				.orElse(Collections.EMPTY_LIST);

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
			
			byte[] peerFilter = (byte[]) args.get("BFpe");
			byte[] seedFilter = (byte[]) args.get("BFse");
			
			if((peerFilter != null && peerFilter.length != BloomFilterBEP33.m/8) || (seedFilter != null && seedFilter.length != BloomFilterBEP33.m/8))
				throw new MessageException("invalid BEP33 filter length", ErrorCode.ProtocolError);
			
			if (dbl != null || nodes != null || nodes6 != null)
			{
				GetPeersResponse resp = new GetPeersResponse(mtid, nodes, nodes6);
				resp.setPeerItems(dbl);
				resp.setToken(token);
				resp.setScrapePeers(peerFilter);
				resp.setScrapeSeeds(seedFilter);
				msg = resp;
				break;
			}
			
			throw new MessageException("Neither nodes nor values in get_peers response",ErrorCode.ProtocolError);
		case UNKNOWN:
			msg = new UnknownTypeResponse(mtid);
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
	
	static <K, T> Optional<T> typedGet(Map<K, ?> map, K key, Class<T> clazz) {
		return Optional.ofNullable(map.get(key)).filter(clazz::isInstance).map(clazz::cast);
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

		byte[] mtid = typedGet(map, MessageBase.TRANSACTION_KEY, byte[].class).filter(tid -> tid.length > 0).orElseThrow(() -> new MessageException("missing or zero-length transaction ID in response", ErrorCode.ProtocolError));
		byte[] hash = typedGet(args,"id", byte[].class).filter(id -> id.length == Key.SHA1_HASH_LENGTH).orElseThrow(() -> new MessageException("missing or invalid node ID", ErrorCode.ProtocolError));
		
		Key id = new Key(hash);

		MessageBase msg = null;

		String requestMethod = getStringFromBytes((byte[]) rawRequestMethod, true);
		
		
		Method method = Optional.ofNullable(MessageBase.messageMethod.get(requestMethod)).orElse(Method.UNKNOWN);
		
		switch(method) {
			case PING:
				msg = new PingRequest();
				break;
			case FIND_NODE:
			case GET_PEERS:
			case GET:
			case UNKNOWN:
				
				hash = Stream.of(args.get("target"), args.get("info_hash")).filter(byte[].class::isInstance).findFirst().map(byte[].class::cast).orElseThrow(() -> {
					if(method == Method.UNKNOWN)
						return new MessageException("Received unknown Message Type: " + requestMethod,ErrorCode.MethodUnknown);
					return new MessageException("missing/invalid target key in request",ErrorCode.ProtocolError);
				});
				
				if (hash.length != Key.SHA1_HASH_LENGTH) {
					throw new MessageException("invalid target key in request",ErrorCode.ProtocolError);
				}
					
				Key target = new Key(hash);
				
				AbstractLookupRequest req;
				
				switch(method) {
					case FIND_NODE:
						req = new FindNodeRequest(target);
						break;
					case GET_PEERS:
						req = new GetPeersRequest(target);
						break;
					case GET:
						req = new GetRequest(target);
						break;
					default:
						req = new UnknownTypeRequest(target);
				}
				
				@SuppressWarnings("unchecked")
				List<byte[]> explicitWants = Optional.ofNullable(args.get("want")).map(castOrThrow(List.class, w -> new MessageException("invalid 'want' parameter, expected a list of byte-strings"))).orElse(null);
						
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
				
				break;
			case PUT:
				
				msg = tap(new PutRequest(), put -> {
					put.value = args.get("v");
					put.pubkey = typedGet(args, "k", byte[].class).orElse(null);
					put.sequenceNumber = typedGet(args, "seq", Long.class).orElse(-1L);
					put.expectedSequenceNumber = typedGet(args, "cas", Long.class).orElse(-1L);
					put.salt = typedGet(args, "salt", byte[].class).orElse(null);
					put.signature = typedGet(args, "sig", byte[].class).orElse(null);
					put.token = typedGet(args, "token", byte[].class).filter(b -> b.length > 0).orElseThrow(() -> new MessageException("missing or invalid token in PUT request"));
					put.validate();
				});
				break;
			case ANNOUNCE_PEER:
				
				hash = typedGet(args, "info_hash", byte[].class).filter(b -> b.length == Key.SHA1_HASH_LENGTH).orElse(null);
				int port = typedGet(args, "port", Long.class).filter(p -> p > 0 && p <= 65535).orElse(0L).intValue();
				byte[] token = typedGet(args, "token", byte[].class).orElse(null);
				boolean isSeed = Long.valueOf(1).equals(args.get("seed"));
				
				if(hash == null || token == null || port == 0)
					throw new MessageException("missing or invalid arguments for announce", ErrorCode.ProtocolError);
				if(token.length == 0)
					throw new MessageException("zero-length token in announce_peer request. see BEP33 for reasons why tokens might not have been issued by get_peers response", ErrorCode.ProtocolError);

				Key infoHash = new Key(hash);

				msg = tap(new AnnounceRequest(infoHash, port, token), ar -> ar.setSeed(isSeed));

				break;
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
