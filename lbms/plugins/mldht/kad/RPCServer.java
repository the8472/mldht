package lbms.plugins.mldht.kad;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageDecoder;
import lbms.plugins.mldht.kad.messages.PingRequest;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import lbms.plugins.mldht.kad.utils.ByteWrapper;
import lbms.plugins.mldht.kad.utils.ResponseTimeoutFilter;

import org.gudy.azureus2.core3.util.BDecoder;

/**
 * @author The_8472, Damokles
 *
 */
public class RPCServer implements Runnable, RPCServerBase {
	
	static Map<InetAddress,RPCServer> interfacesInUse = new HashMap<InetAddress, RPCServer>(); 
	
	private DatagramSocket							sock;
	private DHT										dh_table;
	private ConcurrentMap<ByteWrapper, RPCCallBase>	calls;
	private Queue<RPCCallBase>						call_queue;
	private volatile boolean						running;
	private Thread									thread;
	private int										numReceived;
	private int										numSent;
	private int										port;
	private RPCStats								stats;
	private ResponseTimeoutFilter					timeoutFilter;
	
	private int										serverIndex;

	public RPCServer (DHT dh_table, int port, RPCStats stats, int index) throws SocketException { //, Object parent
		this.port = port;
		this.dh_table = dh_table;
		timeoutFilter = new ResponseTimeoutFilter();
		createSocket();
		calls = new ConcurrentHashMap<ByteWrapper, RPCCallBase>(80,0.75f,3);
		call_queue = new ConcurrentLinkedQueue<RPCCallBase>();
		this.stats = stats;
		this.serverIndex = index;
	}
	
	public DHT getDHT()
	{
		return dh_table;
	}
	
	public static LinkedList<InetAddress> getAvailableAddrs(boolean multiHoming, DHTtype type) {
		LinkedList<InetAddress> addrs = new LinkedList<InetAddress>();
		
		try
		{
			for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces()))
			{
				for (InterfaceAddress ifaceAddr : iface.getInterfaceAddresses())
				{
					if (type.PREFERRED_ADDRESS_TYPE == Inet6Address.class && ifaceAddr.getAddress() instanceof Inet6Address)
					{
						Inet6Address addr = (Inet6Address) ifaceAddr.getAddress();
						// only accept globally reachable IPv6 unicast addresses
						if (addr.isIPv4CompatibleAddress() || addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.isMulticastAddress() || addr.isSiteLocalAddress())
							continue;

						byte[] raw = addr.getAddress();
						// prefer other addresses over teredo
						if (raw[0] == 0x20 && raw[1] == 0x01 && raw[2] == 0x00 && raw[3] == 0x00)
							addrs.addLast(addr);
						else
							addrs.addFirst(addr);
					}
					
					if(type.PREFERRED_ADDRESS_TYPE == Inet4Address.class && ifaceAddr.getAddress() instanceof Inet4Address)
					{
						Inet4Address addr = (Inet4Address) ifaceAddr.getAddress();
						
						if(addr.isLinkLocalAddress() || addr.isLoopbackAddress())
							continue;
						
						addrs.add(addr);
					}
				}
			}
			
			if(type.PREFERRED_ADDRESS_TYPE == Inet4Address.class && !multiHoming)
				addrs.addFirst(InetAddress.getByAddress(new byte[] {0,0,0,0}));
			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
		if(!multiHoming)
			addrs.retainAll(Collections.singleton(addrs.peekFirst()));
		
		addrs.removeAll(interfacesInUse.keySet());
		
		return addrs;
	}
	
	
	private synchronized void createSocket() throws SocketException
	{
		if(sock != null)
		{
			sock.close();
			for(Iterator<RPCServer> it = interfacesInUse.values().iterator();it.hasNext();)
			{
				RPCServer srv = it.next();
				if(srv == this)
					it.remove();
			}
		}
		
		InetAddress addr = getAvailableAddrs(dh_table.getConfig().allowMultiHoming(), dh_table.getType()).peekFirst();
		
		timeoutFilter.reset();

		sock = new DatagramSocket(null);
		sock.setReuseAddress(true);
		sock.bind(new InetSocketAddress(addr, port));
		
		// prevent sockets from being bound to the wrong interfaces
		if(addr == null)
			sock.close();
		else
			interfacesInUse.put(addr, this);
	}
	
	public int getPort() {
		return port;
	}
	
	
	/**
	 * @return external addess, if known (only ipv6 for now)
	 */
	public InetAddress getPublicAddress() {
		if(sock.getLocalAddress() instanceof Inet6Address && !sock.getLocalAddress().isAnyLocalAddress())
			return sock.getLocalAddress();
		return null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#run()
	 */
	public void run() {
		
		int delay = 1;
		
		byte[] buffer = new byte[DHTConstants.RECEIVE_BUFFER_SIZE];
		
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
					createSocket();
					continue;
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
		DHT.logInfo("Stopped RPC Server");
	}
	
	public Key getDerivedID() {
		return dh_table.getNode().getDerivedID(serverIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lbms.plugins.mldht.kad.RPCServerBase#start()
	 */
	public void start () {
		DHT.logInfo("Starting RPC Server");
		running = true;
		thread = new Thread(this, "mlDHT RPC Thread "+dh_table.getType());
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.setDaemon(true);
		thread.start();
		
		// touch it
		dh_table.getNode().getDerivedID(serverIndex);
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#stop()
	 */
	public void stop () {
		running = false;
		sock.close();
		thread = null;
		DHT.logInfo("Stopping RPC Server");
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#doCall(lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	public RPCCall doCall (MessageBase msg) {
		
		RPCCall c = new RPCCall(this, msg);
		
		
		while(true)
		{
			
			if(calls.size() >= DHTConstants.MAX_ACTIVE_CALLS)
			{
				DHT.logInfo("Queueing RPC call, no slots available at the moment");				
				call_queue.add(c);
				break;
			}
			short mtid = (short)DHT.rand.nextInt();
			if(calls.putIfAbsent(new ByteWrapper(mtid),c) == null)
			{
				dispatchCall(c, mtid);
				break;
			}
		}


		return c;
	}
	
	private final RPCCallListener rpcListener = new RPCCallListener() {
		
		public void onTimeout(RPCCallBase c) {
			ByteWrapper w = new ByteWrapper(c.getRequest().getMTID());
			stats.addTimeoutMessageToCount(c.getRequest());
			calls.remove(w);
			dh_table.timeout(c.getRequest());
			doQueuedCalls();
		}
		
		public void onStall(RPCCallBase c) {}
		public void onResponse(RPCCallBase c, MessageBase rsp) {}
	}; 
	
	private void dispatchCall(RPCCallBase call, short mtid)
	{
		MessageBase msg = call.getRequest();
		msg.setMTID(mtid);
		sendMessage(msg);
		call.addListener(rpcListener);
		timeoutFilter.registerCall(call);
		call.start();
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#ping(lbms.plugins.mldht.kad.Key, java.net.InetSocketAddress)
	 */
	public void ping (InetSocketAddress addr) {
		PingRequest pr = new PingRequest();
		pr.setID(dh_table.getNode().getDerivedID(serverIndex));
		pr.setDestination(addr);
		doCall(pr);
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#findCall(byte)
	 */
	public RPCCallBase findCall (byte[] mtid) {
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
	
	// we only decode in the listening thread, so reused the decoder
	private BDecoder decoder = new BDecoder();

	private void handlePacket (DatagramPacket p) {
		numReceived++;
		stats.addReceivedBytes(p.getLength() + dh_table.getType().HEADER_LENGTH);

		if (DHT.isLogLevelEnabled(LogLevel.Verbose)) {
			try {
				DHT.logVerbose(new String(p.getData(), 0, p.getLength(),
						"UTF-8"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			// TODO disable interning once API is available in Az
			Map<String, Object> bedata = decoder.decodeByteArray(p.getData(), 0, p.getLength() /*, false*/);
			MessageBase msg = MessageDecoder.parseMessage(bedata, this);
			if (msg != null) {
				DHT.logDebug("RPC received message ["+p.getAddress().getHostAddress()+"] "+msg.toString());
				stats.addReceivedMessageToCount(msg);
				msg.setOrigin(new InetSocketAddress(p.getAddress(), p.getPort()));
				msg.setServer(this);
				msg.apply(dh_table);
				// erase an existing call
				if (msg.getType() == Type.RSP_MSG
						&& calls.containsKey(new ByteWrapper(msg.getMTID()))) {
					RPCCallBase c = calls.get(new ByteWrapper(msg.getMTID()));
					if(c.getRequest().getDestination().equals(msg.getOrigin()))
					{
						// delete the call, but first notify it of the response
						c.response(msg);
						calls.remove(new ByteWrapper(msg.getMTID()));
						doQueuedCalls();						
					} else
						DHT.logInfo("Response source ("+msg.getOrigin()+") mismatches request destination ("+c.getRequest().getDestination()+"); ignoring response");
				}
			} else
			{
				try {
					DHT.logDebug("RPC received message [" + p.getAddress().getHostAddress() + "] Decode failed msg was:"+new String(p.getData(), 0, p.getLength(),"UTF-8"));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

				
		} catch (IOException e) {
			DHT.log(e, LogLevel.Debug);
		}
	}

	/* (non-Javadoc)
	 * @see lbms.plugins.mldht.kad.RPCServerBase#sendMessage(lbms.plugins.mldht.kad.messages.MessageBase)
	 */
	public void sendMessage (MessageBase msg) {
		try {
			if(msg.getID() == null)
				msg.setID(dh_table.getNode().getDerivedID(serverIndex));
			stats.addSentMessageToCount(msg);
			send(msg.getDestination(), msg.encode());
			DHT.logDebug("RPC send Message: [" + msg.getDestination().getAddress().getHostAddress() + "] "+ msg.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public ResponseTimeoutFilter getTimeoutFilter() {
		return timeoutFilter;
	}

	private void send (InetSocketAddress addr, byte[] msg) throws IOException {
		if (!sock.isClosed()) {
			DatagramPacket p = new DatagramPacket(msg, msg.length);
			p.setSocketAddress(addr);
			try
			{
				sock.send(p);
			} catch (BindException e)
			{
				if(NetworkInterface.getByInetAddress(sock.getLocalAddress()) == null)
				{
					createSocket();
					sock.send(p);
				} else
				{
					throw e;
				}
			}
			stats.addSentBytes(msg.length + dh_table.getType().HEADER_LENGTH);
			numSent++;
		}
	}

	private void doQueuedCalls () {
		while (call_queue.peek() != null && calls.size() < DHTConstants.MAX_ACTIVE_CALLS) {
			RPCCallBase c;

			if((c = call_queue.poll()) == null)
				return;

			short mtid = 0;
			do
			{
				mtid = (short)DHT.rand.nextInt();
			} while (calls.putIfAbsent(new ByteWrapper(mtid), c) != null);

			dispatchCall(c, mtid);
		}
	}

}
