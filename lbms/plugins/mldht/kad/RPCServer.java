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
package lbms.plugins.mldht.kad;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lbms.plugins.mldht.indexer.Selectable;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.*;
import lbms.plugins.mldht.kad.messages.ErrorMessage.ErrorCode;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.kad.utils.ByteWrapper;
import lbms.plugins.mldht.kad.utils.ResponseTimeoutFilter;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;
import lbms.plugins.mldht.utlis.NIOConnectionManager;

import org.gudy.azureus2.core3.util.BDecoder;

/**
 * @author The_8472, Damokles
 *
 */
public class RPCServer {
	
	private InetAddress						addr;
	private DHT										dh_table;
	private RPCServerManager						manager;
	private ConcurrentMap<ByteWrapper, RPCCall>		calls;
	private Queue<RPCCall>							call_queue;
	private Queue<EnqueuedSend>						pipeline;
	private volatile int							numReceived;
	private volatile int							numSent;
	private int										port;
	private RPCStats								stats;
	private ResponseTimeoutFilter					timeoutFilter;
	private Key										derivedId;
	
	
	private boolean isReachable = false;
	private int		numReceivesAtLastCheck = 0;
	private long	timeOfLastReceiveCountChange = 0;
	

	private SocketHandler sel;

	public RPCServer (RPCServerManager manager, InetAddress addr, int port, RPCStats stats) {
		this.port = port;
		this.dh_table = manager.dht;
		timeoutFilter = new ResponseTimeoutFilter();
		pipeline = new ConcurrentLinkedQueue<EnqueuedSend>();
		calls = new ConcurrentHashMap<ByteWrapper, RPCCall>(80,0.75f,3);
		call_queue = new ConcurrentLinkedQueue<RPCCall>();
		this.stats = stats;
		this.addr = addr;
		this.manager = manager;
		// reserve an ID
		derivedId = dh_table.getNode().registerServer(this);
	}
	
	public DHT getDHT()
	{
		return dh_table;
	}
	
	public int getPort() {
		return port;
	}
	
	public InetAddress getBindAddress() {
		return addr;
	}
	
	/**
	 * @return external addess, if known (only ipv6 for now)
	 */
	public InetAddress getPublicAddress() {
		if(sel == null)
			return null;
		InetAddress addr = ((DatagramChannel)sel.getChannel()).socket().getLocalAddress();
		if(dh_table.getType().PREFERRED_ADDRESS_TYPE.isInstance(addr) && AddressUtils.isGlobalUnicast(addr))
			return addr;
		return null;
	}

	
	/*
	public void run() {
		
		int delay = 1;
		
		
		
		while (running)
		{
			DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
			
			try
			{
				if(sock.isClosed())
				{ // don't try to receive on a closed socket, attempt to create a new one instead.
					Thread.sleep(delay * 100);
					if(delay < 256)
						delay <<= 1;
					if(createSocket())
						continue;
					else
						break;
				}
				
				sock.receive(packet);
			} catch (Exception e)
			{
				if (running)
				{
					DHT.log(e, LogLevel.Error);
					sock.close();
				}
				continue;
			}
			
			try
			{
				handlePacket(packet);
				if(delay > 1)
					delay--;
			} catch (Exception e)
			{
				if (running)
					DHT.log(e, LogLevel.Error);
			}
			
		}
		// we fell out of the loop, make sure everything is cleaned up
		destroy();
		DHT.logInfo("Stopped RPC Server");
	}
	*/
	public Key getDerivedID() {
		return derivedId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lbms.plugins.mldht.kad.RPCServerBase#start()
	 */
	public void start() {
		DHT.logInfo("Starting RPC Server");
		sel = new SocketHandler();
	}
	
	public void stop() {
		
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#stop()
	 
	public void destroy () {
		if(running)
			DHT.logInfo("Stopping RPC Server");
		running = false;
		dh_table.getNode().removeServer(this);
		if(sock != null)
			sock.close();
		thread = null;
		
	}*/

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#doCall(lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	void doCall (RPCCall c) {
		
		while(true)
		{
			
			if(calls.size() >= DHTConstants.MAX_ACTIVE_CALLS)
			{
				DHT.logInfo("Queueing RPC call, no slots available at the moment");				
				call_queue.add(c);
				break;
			}
			short mtid = (short)ThreadLocalUtils.getThreadLocalRandom().nextInt();
			if(calls.putIfAbsent(new ByteWrapper(mtid),c) == null)
			{
				dispatchCall(c, mtid);
				break;
			}
		}
	}
	
	private final RPCCallListener rpcListener = new RPCCallListener() {
		
		public void onTimeout(RPCCall c) {
			ByteWrapper w = new ByteWrapper(c.getRequest().getMTID());
			stats.addTimeoutMessageToCount(c.getRequest());
			calls.remove(w);
			dh_table.timeout(c);
			doQueuedCalls();
		}
		
		public void onStall(RPCCall c) {}
		public void onResponse(RPCCall c, MessageBase rsp) {}
	}; 
	
	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#ping(lbms.plugins.mldht.kad.Key, java.net.InetSocketAddress)
	 */
	public void ping (InetSocketAddress addr) {
		PingRequest pr = new PingRequest();
		pr.setID(derivedId);
		pr.setDestination(addr);
		new RPCCall(this, pr).start();
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#findCall(byte)
	 */
	public RPCCall findCall (byte[] mtid) {
		return calls.get(new ByteWrapper(mtid));
	}

	/// Get the number of active calls
	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#getNumActiveRPCCalls()
	 */
	public int getNumActiveRPCCalls () {
		return calls.size();
	}

	/**
	 * @return the numReceived
	 */
	public int getNumReceived () {
		return numReceived;
	}

	/**
	 * @return the numSent
	 */
	public int getNumSent () {
		return numSent;
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#getStats()
	 */
	public RPCStats getStats () {
		return stats;
	}
	
	public void checkReachability(long now) {
		// don't do pings too often if we're not receiving anything (connection might be dead)
		if(numReceived != numReceivesAtLastCheck)
		{
			isReachable = true;
			timeOfLastReceiveCountChange = now;
			numReceivesAtLastCheck = numReceived;
		} else if(now - timeOfLastReceiveCountChange > DHTConstants.REACHABILITY_TIMEOUT)
		{
			isReachable = false;
			timeoutFilter.reset();
		}
	}
	
	public boolean isReachable() {
		return isReachable;
	}
	
	private void handlePacket (ByteBuffer p, SocketAddress soa) {
		InetSocketAddress source = (InetSocketAddress) soa;
		
		// ignore port 0, can't respond to them anyway and responses to requests from port 0 will be useless too
		if(source.getPort() == 0)
			return;

		if (DHT.isLogLevelEnabled(LogLevel.Verbose)) {
			try {
				DHT.logVerbose(new String(p.array(), 0, p.limit(),"UTF-8"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		
		Map<String, Object> bedata = null;
		MessageBase msg = null;
		
		try {
			bedata = ThreadLocalUtils.getDecoder().decodeByteBuffer(p, false);
		} catch(IOException e) {
			DHT.log(e, LogLevel.Debug);
			MessageBase err = new ErrorMessage(new byte[] {0,0,0,0}, ErrorCode.ProtocolError.code,"invalid bencoding: "+e.getMessage());
			err.setDestination(source);
			sendMessage(err);
			return;
		}
		
		try {
			msg = MessageDecoder.parseMessage(bedata, this);
		} catch(MessageException e)
		{
			byte[] mtid = {0,0,0,0};
			if(bedata.containsKey("t") && bedata.get("t") instanceof byte[])
				mtid = (byte[]) bedata.get("t");
			DHT.log(e, LogLevel.Debug);
			MessageBase err = new ErrorMessage(mtid, e.errorCode.code,e.getMessage());
			err.setDestination(source);
			sendMessage(err);
			return;
		}
		
		if(DHT.isLogLevelEnabled(LogLevel.Debug))
			DHT.logDebug("RPC received message ["+source.getAddress().getHostAddress()+"] "+msg.toString());
		stats.addReceivedMessageToCount(msg);
		msg.setOrigin(source);
		msg.setServer(this);
		msg.apply(dh_table);
		
		// check if this is a response to an outstanding request
		RPCCall c = calls.get(new ByteWrapper(msg.getMTID()));
		
		if ((msg.getType() == Type.RSP_MSG || msg.getType() == Type.ERR_MSG) && c != null) {
			if(c.getRequest().getDestination().equals(msg.getOrigin()))
			{
				// delete the call, but first notify it of the response
				c.response(msg);
				calls.remove(new ByteWrapper(msg.getMTID()));
				doQueuedCalls();						
			} else {
				DHT.logInfo("Response source ("+msg.getOrigin()+") mismatches request destination ("+c.getRequest().getDestination()+"); ignoring response");
			}
				
		}

	}
	
	private void fillPipe(EnqueuedSend es) {
		pipeline.add(es);
		sel.updateSelection();
	}
		

	private void dispatchCall(RPCCall call, short mtid)
	{
		MessageBase msg = call.getRequest();
		msg.setMTID(mtid);
		call.addListener(rpcListener);
		timeoutFilter.registerCall(call);
		EnqueuedSend es = new EnqueuedSend(msg);
		es.associatedCall = call;
		fillPipe(es);
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#sendMessage(lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	public void sendMessage (MessageBase msg) {
		fillPipe(new EnqueuedSend(msg));
	}
	
	public ResponseTimeoutFilter getTimeoutFilter() {
		return timeoutFilter;
	}

	/*
	private void send (InetSocketAddress addr, byte[] msg) throws IOException {
		if (!sock.isClosed()) {
			DatagramPacket p = new DatagramPacket(msg, msg.length);
			p.setSocketAddress(addr);
			try
			{
				sock.send(p);
			} catch (IOException e)
			{
				if(sock.isClosed() || NetworkInterface.getByInetAddress(sock.getLocalAddress()) == null)
				{
					createSocket();
					sock.send(p);
				} else
				{
					throw e;
				}
			}

		}
	}*/

	private void doQueuedCalls () {
		while (call_queue.peek() != null && calls.size() < DHTConstants.MAX_ACTIVE_CALLS) {
			RPCCall c;

			if((c = call_queue.poll()) == null)
				return;

			short mtid = 0;
			do
			{
				mtid = (short)ThreadLocalUtils.getThreadLocalRandom().nextInt();
			} while (calls.putIfAbsent(new ByteWrapper(mtid), c) != null);

			dispatchCall(c, mtid);
		}
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append(getDerivedID()).append("\t").append(getPublicAddress()).append(":").append(getPort()).append('\n');
		b.append("rx: ").append(numReceived).append(" tx:").append(numSent).append(" active:").append(getNumActiveRPCCalls()).append(" baseRTT:").append(timeoutFilter.getStallTimeout()).append('\n');
		return b.toString();
	}
	
	private class SocketHandler implements Selectable {
		DatagramChannel channel;

		
		{
			try
			{
				timeoutFilter.reset();
	
				channel = DatagramChannel.open();
				channel.configureBlocking(false);
				channel.socket().setReuseAddress(true);
				channel.socket().bind(new InetSocketAddress(addr, port));
				dh_table.getConnectionManager().register(this);
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		
		NIOConnectionManager connectionManager;

		
		@Override
		public void selectionEvent(SelectionKey key) throws IOException {
			if(key.isValid() && key.isReadable())
				readEvent();
			if(key.isValid() && key.isWritable())
				writeEvent(channel);
				
		}
		
		private void readEvent() throws IOException {
			
			while(true)
			{
				final ByteBuffer buf =  ByteBuffer.allocate(DHTConstants.RECEIVE_BUFFER_SIZE);
				final SocketAddress soa = channel.receive(buf);
				if(soa == null)
					break;
				buf.flip();
				numReceived++;
				stats.addReceivedBytes(buf.limit() + dh_table.getType().HEADER_LENGTH);
				DHT.getScheduler().execute(new Runnable() {
					public void run() {
						handlePacket(buf, soa);
					}
				});
			}
		}
		
		private void writeEvent(DatagramChannel chan)
		{
			EnqueuedSend es;
			while(true)
			{
				es = pipeline.poll();
				if(es == null)
					break;
				try
				{
					ByteBuffer buf = es.getBuffer();
					
					if(chan.send(buf, es.toSend.getDestination()) == 0)
					{
						pipeline.add(es);
						break;
					}
					
					if(es.associatedCall != null)
						es.associatedCall.sent();
					
					stats.addSentMessageToCount(es.toSend);
					stats.addSentBytes(buf.limit() + dh_table.getType().HEADER_LENGTH);
					if(DHT.isLogLevelEnabled(LogLevel.Debug))
						DHT.logDebug("RPC send Message: [" + es.toSend.getDestination().getAddress().getHostAddress() + "] "+ es.toSend.toString());
				} catch (IOException e)
				{
					pipeline.add(es);
					break;
				}
				
				
				numSent++;
			}
	
		}
		
		@Override
		public void registrationEvent(NIOConnectionManager manager) throws IOException {
			connectionManager = manager;
			updateSelection();
		}
		
		@Override
		public SelectableChannel getChannel() {
			return channel;
		}
		
		@Override
		public void doStateChecks(long now) throws IOException {
			if(!channel.isOpen() || !manager.isAddressValid(addr))
			{
				channel.close();
				connectionManager.deRegister(this);
				stop();
				//sel = null;
				return;
			}
			
			updateSelection();
		}
		
		
		
		public void updateSelection() {
			int newSel = SelectionKey.OP_READ;
			if(pipeline.peek() != null)
				newSel |= SelectionKey.OP_WRITE;
			connectionManager.asyncSetSelection(this, newSel);
		}
	}

	private class EnqueuedSend {
		MessageBase toSend;
		RPCCall associatedCall;
		ByteBuffer buf;
		
		public EnqueuedSend(MessageBase msg) {
			toSend = msg;
			if(toSend.getID() == null)
				toSend.setID(getDerivedID());
		}
		
		ByteBuffer getBuffer() throws IOException {
			if(buf != null)
				return buf;
			return buf = ByteBuffer.wrap(toSend.encode());
		}
	}

}
