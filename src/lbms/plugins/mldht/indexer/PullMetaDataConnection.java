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
package lbms.plugins.mldht.indexer;

import java.io.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;

import org.gudy.azureus2.core3.util.BDecoder;
import org.gudy.azureus2.core3.util.BEncoder;
import org.gudy.azureus2.core3.util.SHA1;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import lbms.plugins.mldht.utils.Selectable;

public class PullMetaDataConnection implements Selectable {
	
	public static interface InfohashChecker {
		boolean isInfohashAcceptable(byte[] hash);
	}
	
	public static interface MetaConnectionHandler {
		
		void onTerminate(boolean wasConnected);
		void onConnect();
	}
	
	public static byte[] preamble;
	
	static {
		 try
		{
			preamble = "\u0013BitTorrent protocol".getBytes("ISO-8859-1");
		} catch (UnsupportedEncodingException e)
		{
			// should not happen
			e.printStackTrace();
		}
	}
	// LTEP support, nothing else
	public static final byte[] bitfield = new byte[] {0,0,0,0,0,0x10,0,0};

	public static final int STATE_CONNECTING = 0x01 << 0; //1
	public static final int STATE_BASIC_HANDSHAKING = 0x01 << 1; // 2
	public static final int STATE_LTEP_HANDSHAKING = 0x01 << 2; // 4
	public static final int STATE_GETTING_METADATA = 0x01 << 3; // 8
	public static final int STATE_METADATA_VERIFIED = 0x01 << 4; // 16
	public static final int STATE_CLOSED = 0x01 << 5; // 32
	
	private static final int RCV_TIMEOUT = 25*1000;
	
	private static final int LTEP_HEADER_ID = 20;
	private static final int LTEP_HANDSHAKE_ID = 0;
	private static final int LTEP_LOCAL_META_ID = 1;
	
	private static final int BT_BITFIELD_ID = 5;
	
	private static final int BT_HEADER_LENGTH = 4;
	private static final int BT_MSG_ID_OFFSET = 4; // 0-3 length, 4 id
	private static final int BT_LTEP_HEADER_OFFSET =  5; // 5 ltep id
	
	private static final int META_PIECE_NOT_REQUESTED = 0;
	private static final int META_PIECE_REQUESTED = 1;
	private static final int META_PIECE_DONE = 2;
	
	SocketChannel				channel;
	NIOConnectionManager		connManager;
	boolean						incoming;
	
	Deque<ByteBuffer>			outputBuffers			= new LinkedList<ByteBuffer>();
	ByteBuffer					inputBuffer;

	int							LTEP_REMOTE_META_ID;
	ByteBuffer					metaData;
	InfohashChecker				checker;
	byte[]						infoHash;
	int							infoLength = -1;
	int[]						metaPiecesState;
	int							outstandingRequests;
	int							maxRequests = 10;
	
	long						lastReceivedTime;
	int							consecutiveKeepAlives;
	
	int							state;
	
	BDecoder					decoder = new BDecoder();
	MetaConnectionHandler		metaHandler;
	
	InetSocketAddress			destination;
	String 						remoteClient;
	
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
	
	boolean isState(int mask) {return (state & mask) != 0;}
	
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
			channel.socket().setReuseAddress(true);
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
			connManager.setSelection(this, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE, true);
			if(channel.connect(destination))
				connectEvent();
		} else
		{ // incoming
			connManager.setSelection(this, SelectionKey.OP_READ | SelectionKey.OP_WRITE, true);
			metaHandler.onConnect();
		}
		
		
		
		//System.out.println("attempting connect "+dest);
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
				connManager.setSelection(this, SelectionKey.OP_CONNECT, false);
				setState(STATE_CONNECTING, false);
				metaHandler.onConnect();
				//System.out.println("connection!");
			}
		} catch (IOException e) {
			//System.err.println("connect failed "+e.getMessage());
			terminate("connect failed");
		}
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

			Map<String,Object> ltepHandshake = new HashMap<String, Object>();
			Map<String,Object> messages = new HashMap<String, Object>();
			ltepHandshake.put("m", messages);
			ltepHandshake.put("v","mlDHT Torrent Spider");
			ltepHandshake.put("metadata_size", -1);
			messages.put("ut_metadata", LTEP_LOCAL_META_ID);

			// send handshake
			ByteBuffer handshakeBody = ByteBuffer.wrap(BEncoder.encode(ltepHandshake));
			ByteBuffer handshakeHeader = ByteBuffer.allocate(BT_HEADER_LENGTH + 2);
			handshakeHeader.putInt(handshakeBody.limit() + 2);
			handshakeHeader.put((byte) LTEP_HEADER_ID);
			handshakeHeader.put((byte) LTEP_HANDSHAKE_ID);
			handshakeHeader.flip();
			outputBuffers.addLast(handshakeHeader);
			outputBuffers.addLast(handshakeBody);
			connManager.setSelection(this, SelectionKey.OP_WRITE, true);

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
		int msgID = inputBuffer.get() & 0xFF; 
		if(msgID == LTEP_HEADER_ID)
		{
			// read LTEP msg ID
			msgID = inputBuffer.get() & 0xFF;

			if(isState(STATE_LTEP_HANDSHAKING) && msgID == LTEP_HANDSHAKE_ID)
			{
				//System.out.println("got ltep handshake");

				Map<String,Object> remoteHandshake = BDecoder.decode(inputBuffer.array(), inputBuffer.position(), inputBuffer.remaining());
				Map<String,Object> messages = (Map<String, Object>) remoteHandshake.get("m");
				if(messages == null)
				{
					terminate("no LTEP messages defined");
					return;
				}

				Long metaMsgID = (Long) messages.get("ut_metadata");
				Long metaLength = (Long) remoteHandshake.get("metadata_size");
				Long maxR = (Long) remoteHandshake.get("reqq");
				byte[] ver = (byte[]) remoteHandshake.get("v");
				if(maxR != null)
					maxRequests = maxR.intValue();
				if(ver != null)
					remoteClient = new String(ver,"UTF-8");
				if(metaMsgID != null && metaLength != null)
				{
					int newInfoLength = metaLength.intValue();
					if(newInfoLength < 10)
					{
						terminate("indicated meta length too small to be a torrent");
						return;
					}

					if(metaData == null || newInfoLength != infoLength)
					{
						infoLength = newInfoLength;
						metaData = ByteBuffer.allocate(infoLength);
						metaPiecesState = new int[(int) Math.ceil(infoLength * 1.0 / (16*1024))];
					}

					LTEP_REMOTE_META_ID = metaMsgID.intValue();

					setState(STATE_LTEP_HANDSHAKING, false);
					setState(STATE_GETTING_METADATA, true);


					doMetaRequests();

				} else {
					terminate("no metadata exchange support detected in LTEP");
					return;
				}

			}

			if(isState(STATE_GETTING_METADATA) && msgID == LTEP_LOCAL_META_ID)
			{
				// consumes bytes as necessary for the bencoding
				Map<String, Object> params = decoder.decodeByteBuffer(inputBuffer, false);
				Long type = (Long) params.get("msg_type");
				Long idx = (Long) params.get("piece");

				if(type == 1)
				{ // piece 
					metaPiecesState[idx.intValue()] = META_PIECE_DONE;
					outstandingRequests--;
					// use remaining bytes for metadata
					metaData.position((int) (16*1024 * idx));
					metaData.put(inputBuffer);
					doMetaRequests();
					checkMetaRequests();
				} else if(type == 2)
				{ // reject
					terminate("request was rejected");
					return;
				}
			}

		} /* else if(msgID == BT_BITFIELD_ID && isState(STATE_LTEP_HANDSHAKING))
			{
				// just duplicate whatever they've sent but with 0-bits
				ByteBuffer bitfield = ByteBuffer.allocate(inputBuffer.limit());
				bitfield.putInt(bitfield.limit() - BT_HEADER_LENGTH);
				bitfield.put((byte) BT_BITFIELD_ID);
				bitfield.rewind();
				outputBuffers.addLast(bitfield);
				conHandler.setSelection(this, SelectionKey.OP_WRITE, true);
			}*/


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
		for(int i=0;i<metaPiecesState.length;i++)
		{
			if(metaPiecesState[i] != META_PIECE_NOT_REQUESTED)
				continue;
			if(outstandingRequests >= maxRequests)
				break;
			
			Map<String,Object> req = new HashMap<String, Object>();
			req.put("msg_type", 0);
			req.put("piece", i);
			
			
			ByteBuffer body = ByteBuffer.wrap(BEncoder.encode(req));
			ByteBuffer header = ByteBuffer.allocate(BT_HEADER_LENGTH + 1 + 1);
			header.putInt(2 + body.capacity());
			header.put((byte) LTEP_HEADER_ID);
			header.put((byte) LTEP_REMOTE_META_ID);
			header.flip();
			
			outstandingRequests++;
			metaPiecesState[i] = META_PIECE_REQUESTED;
			outputBuffers.addLast(header);
			outputBuffers.addLast(body);
		}
		
		connManager.setSelection(this, SelectionKey.OP_WRITE, true);
	}
	
	void checkMetaRequests() throws IOException {
		boolean done = true;
		for(int i=0;done && i<metaPiecesState.length;i++)
			done &= metaPiecesState[i] == META_PIECE_DONE;
		
		if(done)
		{
			SHA1 hasher = new SHA1();
			
			metaData.rewind();
			setState(STATE_GETTING_METADATA, false);
			if(!Arrays.equals(infoHash, hasher.digest(metaData)))
			{ // hash check failed
				metaData = null;
				metaPiecesState = null;
				infoLength = -1;
			} else
			{ // success!
				setState(STATE_METADATA_VERIFIED, true);
			}
			terminate("finished meta data exchange");
		}
	}
	
	public void resume(PullMetaDataConnection previousConnection) {
		if(!Arrays.equals(infoHash, previousConnection.infoHash))
			return;
		if(previousConnection.metaData != null)
		{
			metaData = previousConnection.metaData;
			metaPiecesState = previousConnection.metaPiecesState;
			infoLength = previousConnection.infoLength;
			// pieces marked as requested aren't requested anymore since this is a new connection
			for(int i=0;i<metaPiecesState.length;i++)
				if(metaPiecesState[i] == META_PIECE_REQUESTED)
					metaPiecesState[i] = META_PIECE_NOT_REQUESTED;
				
		}
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
		
		if(outputBuffers.size() == 0)
			connManager.setSelection(this, SelectionKey.OP_WRITE, false);
	}
	
	public void doStateChecks(long now) throws IOException {
		if(now - lastReceivedTime > RCV_TIMEOUT || consecutiveKeepAlives >= 2)
			terminate("closing idle connection");
		else if(!channel.isOpen())
			terminate("async close detected");
	}
	
	private void terminate(String reason) throws IOException {
		if(isState(STATE_CLOSED))
			return;
		//if(!isState(STATE_FINISHED) && !isState(STATE_CONNECTING))
			MetaDataGatherer.log("closing connection for "+(infoHash != null ? new Key(infoHash).toString(false) : null)+" to "+destination+"/"+remoteClient+" state:"+state+" reason:"+reason);
		connManager.deRegister(this);
		setState(STATE_CLOSED, true);
		metaHandler.onTerminate(!isState(STATE_CONNECTING));
		channel.close();
		
	}
	
}
