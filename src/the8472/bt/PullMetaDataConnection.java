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
package the8472.bt;

import the8472.bencode.BDecoder;
import the8472.bencode.BEncoder;
import the8472.bt.MetadataPool.Completion;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import lbms.plugins.mldht.utils.Selectable;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public class PullMetaDataConnection implements Selectable {
	
	public static interface InfohashChecker {
		boolean isInfohashAcceptable(byte[] hash);
	}
	
	public static interface MetaConnectionHandler {
		
		void onTerminate(boolean wasConnected);
		void onConnect();
	}
	
	public final static byte[] preamble = "\u0013BitTorrent protocol".getBytes(StandardCharsets.ISO_8859_1);

	
	public static final byte[] bitfield = new byte[8];
	static {
		// ltep
		bitfield[5] |= 0x10;
		// fast extension
		bitfield[7] |= 0x04;
		// BT port
		bitfield[7] |= 0x01;
	}
	

	public static final int STATE_CONNECTING = 0x01 << 0; //1
	public static final int STATE_BASIC_HANDSHAKING = 0x01 << 1; // 2
	public static final int STATE_LTEP_HANDSHAKING = 0x01 << 2; // 4
	public static final int STATE_GETTING_METADATA = 0x01 << 3; // 8
	public static final int STATE_CLOSED = 0x01 << 5; // 32
	
	private static final int RCV_TIMEOUT = 25*1000;
	
	private static final int LTEP_HEADER_ID = 20;
	private static final int LTEP_HANDSHAKE_ID = 0;
	private static final int LTEP_LOCAL_META_ID = 7;
	private static final int LTEP_LOCAL_PEX_ID = 13;
	
	private static final int BT_BITFIELD_ID = 5;
	
	private static final int BT_HEADER_LENGTH = 4;
	private static final int BT_MSG_ID_OFFSET = 4; // 0-3 length, 4 id
	private static final int BT_LTEP_HEADER_OFFSET =  5; // 5 ltep id
	
	SocketChannel				channel;
	NIOConnectionManager		connManager;
	boolean						incoming;
	
	Deque<ByteBuffer>			outputBuffers			= new LinkedList<>();
	ByteBuffer					inputBuffer;

	boolean						remoteSupportsFastExtension;
	boolean						remoteSupportsPort;
	int							ltepRemoteMetadataExchangeMessageId;
	int							ltepRemotePexId;
	public int					dhtPort = -1;
	
	
	MetadataPool				pool;
	InfohashChecker				checker;
	byte[]						infoHash;

	int							outstandingRequests;
	int							maxRequests = 1;
	
	long						lastReceivedTime;
	int							consecutiveKeepAlives;
	
	int							state;
	
	BDecoder					decoder = new BDecoder();
	MetaConnectionHandler		metaHandler;
	
	InetSocketAddress			destination;
	String 						remoteClient;
	
	public Consumer<List<InetSocketAddress>> pexConsumer = (x) -> {};
	public IntFunction<MetadataPool> 	poolGenerator = (i) -> new MetadataPool(i);
	
	static PrintWriter idWriter;
	
	static {
		try
		{
			idWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./peer_ids.log",true))),true);
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	void setState(int mask, boolean onOff) {
		if(onOff)
			state |= mask;
		else
			state &= ~mask;
	}
	
	public boolean isState(int mask) {return (state & mask) != 0;}
	
	public void setListener(MetaConnectionHandler handler) {
		metaHandler = handler;
	}
	
	// incoming
	public PullMetaDataConnection(SocketChannel chan)
	{
		channel = chan;
		incoming = true;

		try
		{
			channel.configureBlocking(false);
		} catch (IOException e)
		{
			DHT.log(e, LogLevel.Error);
		}
		
		destination = (InetSocketAddress) chan.socket().getRemoteSocketAddress();
		setState(STATE_BASIC_HANDSHAKING,true);
	}
	
	
	// outgoing
	public PullMetaDataConnection(byte[] infoHash, InetSocketAddress dest) {
		this.infoHash = infoHash;
		this.destination = dest;

		try
		{
			channel = SocketChannel.open();
			//channel.socket().setReuseAddress(true);
			channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
			channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
			channel.configureBlocking(false);
			//channel.bind(new InetSocketAddress(49002));



		} catch (IOException e)
		{
			DHT.log(e, LogLevel.Error);
		}
		
		setState(STATE_CONNECTING, true);
		setState(STATE_BASIC_HANDSHAKING, true);
		sendBTHandshake();
	}
	
	private void sendBTHandshake() {
		ByteBuffer outputBuffer = ByteBuffer.allocate(20+8+20+20);
		byte[] peerID = new byte[20];
		ThreadLocalUtils.getThreadLocalRandom().nextBytes(peerID);

		outputBuffer.put(preamble);
		outputBuffer.put(bitfield);
		outputBuffer.put(infoHash);
		outputBuffer.put(peerID);


		outputBuffer.flip();
		outputBuffers.addLast(outputBuffer);
	}
	
	public SelectableChannel getChannel() {
		return channel;
	}
	
	public void registrationEvent(NIOConnectionManager manager, SelectionKey key) throws IOException {
		connManager = manager;
		lastReceivedTime = System.currentTimeMillis();

		if(isState(STATE_CONNECTING))
		{
			if(channel.connect(destination))
				connectEvent();
		} else
		{ // incoming
			metaHandler.onConnect();
		}
		
		connManager.interestOpsChanged(this);
		
		
		
		//System.out.println("attempting connect "+dest);
	}
	
	@Override
	public int calcInterestOps() {
		int ops = SelectionKey.OP_READ;
		if(isState(STATE_CONNECTING))
			ops |= SelectionKey.OP_CONNECT;
		if(!outputBuffers.isEmpty())
			ops |= SelectionKey.OP_WRITE;
					
		return ops;
	}
	
	@Override
	public void selectionEvent(SelectionKey key) throws IOException {
		if(key.isConnectable())
			connectEvent();
		if(key.isValid() && key.isReadable())
			canReadEvent();
		if(key.isValid() && key.isWritable())
			canWriteEvent();
	}
	
	
	public void connectEvent() throws IOException {
		try {
			if(channel.isConnectionPending() && channel.finishConnect())
			{
				connManager.interestOpsChanged(this);;
				setState(STATE_CONNECTING, false);
				metaHandler.onConnect();
				//System.out.println("connection!");
			}
		} catch (IOException e) {
			//System.err.println("connect failed "+e.getMessage());
			terminate("connect failed");
		}
	}
	
	
	public MetadataPool getMetaData() {
		return pool;
	}
	
	private void processInput() throws IOException {
		inputBuffer.flip();

		if(isState(STATE_BASIC_HANDSHAKING))
		{
			boolean connectionMatches = true;
			byte[] temp = new byte[20];
			byte[] otherBitfield = new byte[8];

			// check preamble
			inputBuffer.get(temp);
			connectionMatches &= Arrays.equals(temp, preamble);


			// check LTEP support
			inputBuffer.get(otherBitfield);
			connectionMatches &= (otherBitfield[5] & 0x10) != 0;
			
			remoteSupportsFastExtension = (otherBitfield[7] & 0x04) != 0;
			remoteSupportsPort	= (otherBitfield[7] & 0x01) != 0;

			// check infohash
			inputBuffer.get(temp);
			if(infoHash != null) {
				connectionMatches &= Arrays.equals(temp, infoHash);
			} else {
				infoHash = temp.clone();
				if(!checker.isInfohashAcceptable(infoHash))
				{
					terminate("currently not handling this infohash");
					return;
				}
			}


			// check peer ID
			inputBuffer.get(temp);
			// log
			idWriter.append(System.currentTimeMillis()+ " " + new Key(temp).toString(false) + " " + destination.getAddress().getHostAddress()+ " \n");
			if(temp[0] == '-' && temp[1] == 'S' && temp[2] == 'D' && temp[3] == '0' && temp[4] == '1' && temp[5] == '0' &&  temp[6] == '0' && temp[7] == '-')
				connectionMatches = false; // xunlei claims ltep support but doesn't actually do it


			if(!connectionMatches)
			{
				terminate("connction mismatch");
				return;
			}

			// start parsing BT messages
			if(incoming)
				sendBTHandshake();

			Map<String,Object> ltepHandshake = new HashMap<>();
			Map<String,Object> messages = new HashMap<>();
			ltepHandshake.put("m", messages);
			ltepHandshake.put("v","mlDHT metadata fetcher");
			ltepHandshake.put("metadata_size", 0);
			ltepHandshake.put("reqq", 256);
			messages.put("ut_metadata", LTEP_LOCAL_META_ID);
			messages.put("ut_pex", LTEP_LOCAL_PEX_ID);
			
			// send handshake
			BEncoder encoder = new BEncoder();
			ByteBuffer handshakeBody = encoder.encode(ltepHandshake, 1024);

			ByteBuffer handshakeHeader = ByteBuffer.allocate(BT_HEADER_LENGTH + 2);
			handshakeHeader.putInt(handshakeBody.limit() + 2);
			handshakeHeader.put((byte) LTEP_HEADER_ID);
			handshakeHeader.put((byte) LTEP_HANDSHAKE_ID);
			handshakeHeader.flip();
			outputBuffers.addLast(handshakeHeader);
			outputBuffers.addLast(handshakeBody);
			/*
			if(remoteSupportsFastExtension) {
				ByteBuffer haveNone = ByteBuffer.allocate(5);
				haveNone.put(3, (byte) 1);
				haveNone.put(4, (byte) 0x0f);
				outputBuffers.addLast(haveNone);
			}
			if(remoteSupportsPort && dhtPort != -1) {
				ByteBuffer btPort = ByteBuffer.allocate(7);
				btPort.putInt(3);
				btPort.put((byte) 0x09);
				btPort.putShort((short) dhtPort);
				btPort.flip();
				outputBuffers.addLast(btPort);
			}*/
			canWriteEvent();

			//System.out.println("got basic handshake");

			inputBuffer.position(0);
			inputBuffer.limit(BT_HEADER_LENGTH);
			setState(STATE_BASIC_HANDSHAKING, false);
			setState(STATE_LTEP_HANDSHAKING, true);
			return;
		}

		// parse BT header
		if(inputBuffer.limit() == BT_HEADER_LENGTH)
		{
			int msgLength = inputBuffer.getInt();

			// keepalive... wait for next msg
			if(msgLength == 0)
			{
				//terminate("we don't wait for keep-alives");
				consecutiveKeepAlives++;
				inputBuffer.flip();
				return;
			}

			consecutiveKeepAlives = 0;

			int newLength = BT_HEADER_LENGTH + msgLength;

			if(newLength > inputBuffer.capacity() || newLength < 0)
			{
				terminate("message size too large or < 0");
				return;
			}

			// read payload
			inputBuffer.limit(newLength);
			return;
		}

		// skip header, we already processed that
		inputBuffer.position(4);

		// received a full message, reset timeout
		lastReceivedTime = System.currentTimeMillis();



		// read BT msg ID
		int btMsgID = inputBuffer.get() & 0xFF;
		if(btMsgID == LTEP_HEADER_ID)
		{
			// read LTEP msg ID
			int ltepMsgID = inputBuffer.get() & 0xFF;

			if(isState(STATE_LTEP_HANDSHAKING) && ltepMsgID == LTEP_HANDSHAKE_ID)
			{
				//System.out.println("got ltep handshake");
				
				BDecoder decoder = new BDecoder();
				Map<String,Object> remoteHandshake = decoder.decode(inputBuffer);
				Map<String,Object> messages = (Map<String, Object>) remoteHandshake.get("m");
				if(messages == null)
				{
					terminate("no LTEP messages defined");
					return;
				}

				Long metaMsgID = (Long) messages.get("ut_metadata");
				Long pexMsgID = (Long) messages.get("ut_pex");
				Long metaLength = (Long) remoteHandshake.get("metadata_size");
				Long maxR = (Long) remoteHandshake.get("reqq");
				byte[] ver = (byte[]) remoteHandshake.get("v");
				//if(maxR != null)
					//maxRequests = maxR.intValue();
				if(ver != null)
					remoteClient = new String(ver,StandardCharsets.UTF_8);
				if(pexMsgID != null)
					ltepRemotePexId = pexMsgID.intValue();
				if(metaMsgID != null && metaLength != null)
				{
					int newInfoLength = metaLength.intValue();
					if(newInfoLength < 10) {
						terminate("indicated meta length too small to be a torrent");
						return;
					}
					
					// 30MB ought to be enough for everyone!
					if(newInfoLength > 30*1024*1024) {
						terminate("indicated meta length too large ("+newInfoLength+"), might be a resource exhaustion attack");
						return;
					}
					
					pool = poolGenerator.apply(newInfoLength);

					ltepRemoteMetadataExchangeMessageId = metaMsgID.intValue();

					setState(STATE_GETTING_METADATA, true);


					doMetaRequests();

				}
				
				if(pexMsgID == null && (metaMsgID == null || metaLength == null)){
					terminate("neither metadata exchange support nor pex detected in LTEP -> peer is useless");
					return;
				}
				
				// send 1 keep-alive
				//outputBuffers.add(ByteBuffer.wrap(new byte[4]));
				
				setState(STATE_LTEP_HANDSHAKING, false);
			}
			
			if(!isState(STATE_LTEP_HANDSHAKING) && ltepMsgID == LTEP_LOCAL_PEX_ID) {
				BDecoder decoder = new BDecoder();
				Map<String, Object> params = decoder.decode(inputBuffer);
				
				pexConsumer.accept(AddressUtils.unpackCompact((byte[])params.get("added"), Inet4Address.class));
				pexConsumer.accept(AddressUtils.unpackCompact((byte[])params.get("added6"), Inet6Address.class));
			}

			if(isState(STATE_GETTING_METADATA) && ltepMsgID == LTEP_LOCAL_META_ID)
			{
				// consumes bytes as necessary for the bencoding
				BDecoder decoder = new BDecoder();
				Map<String, Object> params = decoder.decode(inputBuffer);
				Long type = (Long) params.get("msg_type");
				Long idx = (Long) params.get("piece");
				
				if(type == 1)
				{ // piece
					outstandingRequests--;
					
					ByteBuffer chunk = ByteBuffer.allocate(inputBuffer.remaining());
					chunk.put(inputBuffer);
					pool.addBuffer(idx.intValue(), chunk);
					
					doMetaRequests();
					checkMetaRequests();
				} else if(type == 2)
				{ // reject
					pool.releasePiece(idx.intValue());
					terminate("request was rejected");
					return;
				}
			}

		}
		
		/*
		if(btMsgID == BT_BITFIELD_ID & !remoteSupportsFastExtension)
			{
				// just duplicate whatever they've sent but with 0-bits
				ByteBuffer bitfield = ByteBuffer.allocate(inputBuffer.limit());
				bitfield.putInt(bitfield.limit() - BT_HEADER_LENGTH);
				bitfield.put((byte) BT_BITFIELD_ID);
				bitfield.rewind();
				outputBuffers.addLast(bitfield);
				canWriteEvent();
			}
		*/


		// parse next BT header
		inputBuffer.position(0);
		inputBuffer.limit(BT_HEADER_LENGTH);
		
	}
	
	public void canReadEvent() throws IOException {
		int bytesRead = 0;
		
		if(inputBuffer == null)
		{
			inputBuffer = ByteBuffer.allocate(32 * 1024);
			// await BT handshake on first allocation since this has to be the first read
			inputBuffer.limit(20+8+20+20);
		}
		
		do {
			try
			{
				bytesRead = channel.read(inputBuffer);
			} catch (IOException e)
			{
				terminate("exception on read, cause: "+e.getMessage());
			}
			
			if(bytesRead == -1)
				terminate("reached end of stream on read");
			// message complete as far as we need it
			else if(inputBuffer.remaining() == 0)
				processInput();
		} while(bytesRead > 0 && !isState(STATE_CLOSED));
		

		
		
	}
	
	void doMetaRequests() throws IOException {
		if(!isState(STATE_GETTING_METADATA))
			return;
		
		while(outstandingRequests <= maxRequests)
		{
			int idx = pool.reservePiece(this);
			

			if(idx < 0)
				break;
			
			Map<String,Object> req = new HashMap<>();
			req.put("msg_type", 0);
			req.put("piece", idx);
			
			BEncoder encoder = new BEncoder();
			ByteBuffer body = encoder.encode(req, 512);
			ByteBuffer header = ByteBuffer.allocate(BT_HEADER_LENGTH + 1 + 1);
			header.putInt(2 + body.remaining());
			header.put((byte) LTEP_HEADER_ID);
			header.put((byte) ltepRemoteMetadataExchangeMessageId);
			header.flip();
			
			outstandingRequests++;
			
			outputBuffers.addLast(header);
			outputBuffers.addLast(body);
		}
		
		canWriteEvent();
	}
	
	void checkMetaRequests() throws IOException {
		if(pool == null)
			return;

		pool.checkComletion(infoHash);
		
		if(pool.status != Completion.PROGRESS)
			terminate("meta data exchange finished or failed");
	}


	public void canWriteEvent() throws IOException {
		ByteBuffer outBuf = outputBuffers.pollFirst();
		try
		{
			while(outBuf != null)
			{
				channel.write(outBuf);
				if(outBuf.hasRemaining()) {
					outputBuffers.addFirst(outBuf);
					// socket buffer full, update selector
					connManager.interestOpsChanged(this);
					outBuf = null;
				} else {
					outBuf = outputBuffers.pollFirst();
				}
			}
		} catch (IOException e)
		{
			terminate("error on write, cause: "+e.getMessage());
			return;
		}
		
		// drained queue -> update selector
		if(outputBuffers.size() == 0)
			connManager.interestOpsChanged(this);
	}
	
	public void doStateChecks(long now) throws IOException {
		// connections sharing a pool might get stalled if no more requests are left
		doMetaRequests();
		// hash check may have finished or failed due to other pool members
		checkMetaRequests();
		
		if(now - lastReceivedTime > RCV_TIMEOUT || consecutiveKeepAlives >= 2) {
			terminate("closing idle connection "+Integer.toBinaryString(state)+" "+outstandingRequests+" "+outputBuffers.size()+" "+inputBuffer);
		}
			
		else if(!channel.isOpen())
			terminate("async close detected");
	}
	
	public void terminate(String reason) throws IOException {
		if(isState(STATE_CLOSED))
			return;
		//if(!isState(STATE_FINISHED) && !isState(STATE_CONNECTING))
			//MetaDataGatherer.log("closing connection for "+(infoHash != null ? new Key(infoHash).toString(false) : null)+" to "+destination+"/"+remoteClient+" state:"+state+" reason:"+reason);
		if(pool != null)
			pool.deRegister(this);
		if(connManager != null)
			connManager.deRegister(this);
		setState(STATE_CLOSED, true);
		metaHandler.onTerminate(!isState(STATE_CONNECTING));
		channel.close();
	}
	
}
