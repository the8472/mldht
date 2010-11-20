package lbms.plugins.mldht.kad.utils;

import java.net.*;
import java.util.Collections;
import java.util.LinkedList;

import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.PeerAddressDBItem;

public class AddressUtils {
	
	public static boolean isBogon(PeerAddressDBItem item)
	{
		return isBogon(item.getInetAddress(), item.getPort());
	}
	
	public static boolean isBogon(InetSocketAddress addr)
	{
		return isBogon(addr.getAddress(),addr.getPort());
	}
	
	public static boolean isBogon(InetAddress addr, int port)
	{
		return !(port > 0 && port <= 0xFFFF && isGlobalUnicast(addr));
	}
	
	public static boolean isGlobalUnicast(InetAddress addr)
	{
		return !(addr.isAnyLocalAddress() || addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.isMulticastAddress() || addr.isSiteLocalAddress());
	}

	public static LinkedList<InetAddress> getAvailableAddrs(boolean multiHoming, Class<? extends InetAddress> type) {
		LinkedList<InetAddress> addrs = new LinkedList<InetAddress>();
		
		try
		{
			for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces()))
			{
				for (InterfaceAddress ifaceAddr : iface.getInterfaceAddresses())
				{
					if (type == Inet6Address.class && ifaceAddr.getAddress() instanceof Inet6Address)
					{
						Inet6Address addr = (Inet6Address) ifaceAddr.getAddress();
						// only accept globally reachable IPv6 unicast addresses
						if (addr.isIPv4CompatibleAddress() || !isGlobalUnicast(addr))
							continue;
	
						byte[] raw = addr.getAddress();
						// prefer other addresses over teredo
						if (raw[0] == 0x20 && raw[1] == 0x01 && raw[2] == 0x00 && raw[3] == 0x00)
							addrs.addLast(addr);
						else
							addrs.addFirst(addr);
					}
					
					if(type == Inet4Address.class && ifaceAddr.getAddress() instanceof Inet4Address)
					{
						Inet4Address addr = (Inet4Address) ifaceAddr.getAddress();

						// with multihoming we only accept globals
						if(multiHoming && !isGlobalUnicast(addr))
							continue;
						// without multihoming we'll accept site-local addresses too, since they could be NATed
						if(addr.isLinkLocalAddress() || addr.isLoopbackAddress())
							continue;
						
						addrs.add(addr);
					}
				}
			}
			
			// single-homed? just return the any local address for v4, that's easier than determining a correct bind address
			if(type == Inet4Address.class && !multiHoming)
				addrs.addFirst(InetAddress.getByAddress(new byte[] {0,0,0,0}));
			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
		if(!multiHoming)
			addrs.retainAll(Collections.singleton(addrs.peekFirst()));
		
		
		
		return addrs;
	}
	
}
