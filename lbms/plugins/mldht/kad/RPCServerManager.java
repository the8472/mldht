package lbms.plugins.mldht.kad;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

public class RPCServerManager {
	
	boolean destroyed;
	
	public RPCServerManager(DHT dht) {
		this.dht = dht;
	}
	
	DHT dht;
	private ConcurrentHashMap<InetAddress,RPCServer> interfacesInUse = new ConcurrentHashMap<InetAddress, RPCServer>();
	private volatile RPCServer[] activeServers = new RPCServer[0];
	private Set<InetAddress> validAddresses;
	
	public void refresh(long now) {
		if(destroyed)
			return;
		List<InetAddress> addrs = AddressUtils.getAvailableAddrs(dht.config.allowMultiHoming(), dht.getType().PREFERRED_ADDRESS_TYPE);
		validAddresses = new HashSet<InetAddress>(addrs);
		addrs.removeAll(interfacesInUse.keySet());
		for(InetAddress addr : addrs)
		{
			RPCServer srv = new RPCServer(this,addr,dht.config.getListeningPort(), dht.serverStats);
			srv.start();
			interfacesInUse.put(addr, srv);
		}
		
		List<RPCServer> newServers = new ArrayList<RPCServer>(interfacesInUse.values().size());
		for(Iterator<RPCServer> it = interfacesInUse.values().iterator();it.hasNext();)
		{
			RPCServer srv = it.next();
			srv.checkReachability(now);
			if(srv.isReachable())
				newServers.add(srv);
		}
		
		activeServers = newServers.toArray(new RPCServer[newServers.size()]);
	}
	
	public boolean isAddressValid(InetAddress addr)
	{
		return validAddresses.contains(addr);
	}
	
	void serverRemoved(RPCServer srv) {
		interfacesInUse.remove(srv.getBindAddress(),srv);
		refresh(System.currentTimeMillis());
	}
	
	public void destroy() {
		destroyed = true;
		for(RPCServer srv : new ArrayList<RPCServer>(interfacesInUse.values()))
			srv.stop();
	}
	
	public int getServerCount() {
		return interfacesInUse.size();
	}
	
	public int getActiveServerCount()
	{
		return activeServers.length;
	}
	
	public RPCServer getRandomActiveServer(boolean fallback)
	{
		RPCServer[] srvs = activeServers;
		if(srvs.length == 0)
			return fallback ? getRandomServer() : null;
		return srvs[ThreadLocalUtils.getThreadLocalRandom().nextInt(srvs.length)];
	}
	
	/**
	 * this method is relatively expensive... don't call it frequently
	 */
	public RPCServer getRandomServer() {
		List<RPCServer> servers = getAllServers();
		if(servers.isEmpty())
			return null;
		return servers.get(ThreadLocalUtils.getThreadLocalRandom().nextInt(servers.size()));
	}
	
	public List<RPCServer> getAllServers() {
		return new ArrayList<RPCServer>(interfacesInUse.values());
	}
	
}
