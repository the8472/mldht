package the8472.mldht;

import static the8472.bencode.Utils.str2buf;
import static the8472.utils.Functional.tap;
import static the8472.utils.Functional.unchecked;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.MessageBase.Method;
import lbms.plugins.mldht.kad.messages.MessageBase.Type;
import the8472.utils.ConfigReader;
import the8472.utils.XMLUtils;

public class PassiveRedisIndexer implements Component {
	
	private TransferQueue<ByteBuffer> writeQueue = new LinkedTransferQueue<>();
	
	Thread r = new Thread(this::read, "redis-reader");
	Thread w = new Thread(this::write, "redis-writer");
	SocketChannel chan;
	private volatile boolean running = true;
	
	ConfigReader config;
	
	public void start(Collection<DHT> dhts, ConfigReader config)  {
		this.config = config;
		
		dhts.forEach((dht) -> {
			dht.addIncomingMessageListener(this::incomingMessage);
		});
		
		try {
			chan = SocketChannel.open();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		w.setDaemon(true);
		w.start();
		
	}
	
	public void stop() {
		// TODO Auto-generated method stub
		running = false;
	}
	
	private static final String TTL = Integer.toString(2*24*3600);
	
	private void incomingMessage(DHT dht, MessageBase msg) {
		if(!running)
			return;
		
		if(msg.getType() == Type.REQ_MSG && msg.getMethod() == Method.GET_PEERS)
		{
			GetPeersRequest req = (GetPeersRequest) msg;
			long now = System.currentTimeMillis();
			Key k =	req.getTarget();
			String ipAddr = req.getOrigin().getAddress().getHostAddress();
			String key = k.toString(false);
			
			StringBuilder b = new StringBuilder();
			
			
			
			// zadd <hash> <timestamp> <ip>
			b.append("*4\r\n");

			b.append("$4\r\n");
			b.append("ZADD\r\n");

			b.append("$40\r\n");
			b.append(key).append("\r\n");
			
			String intAsString = Long.toString(now);
			b.append('$').append(intAsString.length()).append("\r\n");
			b.append(intAsString).append("\r\n");

			b.append('$').append(ipAddr.length()).append("\r\n");
			b.append(ipAddr).append("\r\n");
			
			// expire <hash> <ttl>
			
			b.append("*3\r\n");
			
			b.append("$6\r\n");
			b.append("EXPIRE\r\n");

			b.append("$40\r\n");
			b.append(key).append("\r\n");
			
			b.append('$').append(TTL.length()).append("\r\n");
			b.append(TTL).append("\r\n");
			
			
			writeQueue.add(str2buf(b.toString()));
			
		}
	}
	
	
	static private final Map<String,String> namespaces = tap(new HashMap<>(), m -> m.put("xsi","http://www.w3.org/2001/XMLSchema-instance"));
	
	private InetAddress getAddress() {
		return config.get(XMLUtils.buildXPath("//components/component[@xsi:type='mldht:redisIndexerType']/address",namespaces)).flatMap(unchecked(str -> Optional.of(InetAddress.getByName(str)))).get();
	}
	
	private void write() {
		try {
			ByteBuffer current = writeQueue.take();
			
			chan.connect(new InetSocketAddress(getAddress(),6379));

			r.setDaemon(true);
			r.start();

			
			while(running) {
				if(current.remaining() == 0)
					current = writeQueue.take();
				chan.write(current);
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void read() {
		ByteBuffer current = ByteBuffer.allocateDirect(4096);
		
		try {
			while(running) {
				// we just dump reads into oblivion
				current.clear();
				chan.read(current);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	};

}
