package lbms.plugins.mldht.kad.utils;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import lbms.plugins.mldht.kad.PeerAddressDBItem;
import the8472.utils.Arrays;

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
		if(addr instanceof Inet4Address && addr.getAddress()[0] == 0)
			return false;
		return !(addr.isAnyLocalAddress() || addr.isLinkLocalAddress() || addr.isLoopbackAddress() || addr.isMulticastAddress() || addr.isSiteLocalAddress());
	}
	
	public static byte[] packAddress(InetSocketAddress addr) {
		byte[] result = null;
		int port = addr.getPort();
		
		if(addr.getAddress() instanceof Inet4Address) {
			result = new byte[6];
		}
		
		if(addr.getAddress() instanceof Inet6Address) {
			result = new byte[18];
		}
		
		ByteBuffer buf = ByteBuffer.wrap(result);
		buf.put(addr.getAddress().getAddress());
		buf.putChar((char)(addr.getPort() & 0xffff));
		
		return result;
	}
	
	public static InetSocketAddress unpackAddress(byte[] raw) {
		if((raw.length != 6 && raw.length != 18) || raw == null)
			return null;
		ByteBuffer buf = ByteBuffer.wrap(raw);
		byte[] rawIP = new byte[raw.length - 2];
		buf.get(rawIP);
		int port = buf.getChar();
		InetAddress ip;
		try {
			ip = InetAddress.getByAddress(rawIP);
		} catch (UnknownHostException e) {
			return null;
		}
		return new InetSocketAddress(ip, port);
	}
	
	

	public static List<InetAddress> getAvailableGloballyRoutableAddrs(Class<? extends InetAddress> type) {
		
		LinkedList<InetAddress> addrs = new LinkedList<InetAddress>();
		
		try
		{
			for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces()))
			{
				if(!iface.isUp() || iface.isLoopback())
					continue;
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

						if(!isGlobalUnicast(addr))
							continue;
					
						addrs.add(addr);
					}
				}
			}
			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
		Collections.sort(addrs, (a, b) -> Arrays.compareUnsigned(a.getAddress(), b.getAddress()));
		
		
		return addrs;
	}
	
	public static boolean isValidBindAddress(InetAddress addr) {
		// we don't like them them but have to allow them
		if(addr.isAnyLocalAddress())
			return true;
		try {
			NetworkInterface iface = NetworkInterface.getByInetAddress(addr);
			if(iface == null)
				return false;
			return iface.isUp() && !iface.isLoopback();
		} catch (SocketException e) {
			return false;
		}
	}
	
	public static InetAddress getAnyLocalAddress(Class<? extends InetAddress> type) {
		try {
			if(type == Inet6Address.class)
				return InetAddress.getByAddress(new byte[16]);
			if(type == Inet4Address.class)
				return InetAddress.getByAddress(new byte[4]);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		throw new RuntimeException("this shouldn't happen");
	}
	
	public static InetAddress getDefaultRoute(Class<? extends InetAddress> type) {
		InetAddress target = null;
		
		try(DatagramChannel chan=DatagramChannel.open()) {
			if(type == Inet4Address.class)
				target = InetAddress.getByAddress(new byte[] {8,8,8,8});
			if(type == Inet6Address.class)
				target = InetAddress.getByName("2001:4860:4860::8888");
			
			//chan.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			//chan.configureBlocking(true);
			//chan.bind(new InetSocketAddress(getAnyLocalAddress(type),0) );

			chan.connect(new InetSocketAddress(target,63));
			
			InetSocketAddress soa = (InetSocketAddress) chan.getLocalAddress();
			InetAddress local = soa.getAddress();
			
			if(type.isInstance(local) && !local.isAnyLocalAddress())
				return local;
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
}
