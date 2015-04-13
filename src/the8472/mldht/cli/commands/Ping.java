package the8472.mldht.cli.commands;

import static the8472.bencode.Utils.buf2str;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCCallListener;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;
import the8472.mldht.cli.CommandProcessor;

public class Ping extends CommandProcessor {
	
	InetSocketAddress target;
	
	ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
	
	{
		timer.setKeepAliveTime(2, TimeUnit.SECONDS);
		timer.allowCoreThreadTimeOut(true);
	}
	
	AtomicInteger iteration = new AtomicInteger();
		
	
	@Override
	protected void process() {
		String ip = buf2str(ByteBuffer.wrap(arguments.get(0)));
		int port = Integer.valueOf(buf2str(ByteBuffer.wrap(arguments.get(1))));
		
		InetAddress addr;
		
		try {
			addr = InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			handleException(e);
			return;
		}
		
		target = new InetSocketAddress(addr, port);
		
		println("PING " + target);
		
		doPing();
	}
	
	void doPing() {
		if(!isRunning())
			return;
		
		DHT dht = dhts.stream().filter(d -> d.getType().PREFERRED_ADDRESS_TYPE == target.getAddress().getClass()).findAny().get();
		
		RPCServer srv = dht.getServerManager().getRandomActiveServer(true);
		PingRequest req = new PingRequest();
		req.setDestination(target);
		RPCCall call = new RPCCall(req);
		call.addListener(new RPCCallListener() {
			
			int counter = iteration.incrementAndGet();
			
			@Override
			public void onTimeout(RPCCall c) {
				println("#"+counter+": timed out");
				timer.schedule(Ping.this::doPing, 1, TimeUnit.SECONDS);
			}
			
			@Override
			public void onStall(RPCCall c) {}
			
			@Override
			public void onResponse(RPCCall c, MessageBase rsp) {
				println("#"+counter+" response time=" + c.getRTT() + "ms");
				timer.schedule(Ping.this::doPing, 1, TimeUnit.SECONDS);
				
			}
		});
		srv.doCall(call);
	}

}
