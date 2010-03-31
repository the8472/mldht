package lbms.plugins.mldht.kad;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import lbms.plugins.mldht.kad.DHT.DHTtype;

public class PeerAddressDBItem extends DBItem {
	
	
	boolean seed;
	
	public PeerAddressDBItem(byte[] data, boolean isSeed) {
		super(data);
		if(data.length != DHTtype.IPV4_DHT.ADDRESS_ENTRY_LENGTH && data.length != DHTtype.IPV6_DHT.ADDRESS_ENTRY_LENGTH)
			throw new IllegalArgumentException("byte array length does not match ipv4 or ipv6 raw InetAddress+Port length");
		seed = isSeed;
	}
	
	public InetAddress getInetAddress() {
		try
		{
			if (item.length == DHTtype.IPV4_DHT.ADDRESS_ENTRY_LENGTH)
				return InetAddress.getByAddress(Arrays.copyOf(item, 4));
			if (item.length == DHTtype.IPV6_DHT.ADDRESS_ENTRY_LENGTH)
				return InetAddress.getByAddress(Arrays.copyOf(item, 16));
		} catch (UnknownHostException e)
		{
			// should not happen
			e.printStackTrace();
		}
		
		return null;
	}
	
	public String getAddressAsString() {
		return getInetAddress().getHostAddress();
	}
	
	public Class<? extends InetAddress> getAddressType() {
		if(item.length == DHTtype.IPV4_DHT.ADDRESS_ENTRY_LENGTH)
			return DHTtype.IPV4_DHT.PREFERRED_ADDRESS_TYPE;
		if(item.length == DHTtype.IPV6_DHT.ADDRESS_ENTRY_LENGTH)
			return DHTtype.IPV6_DHT.PREFERRED_ADDRESS_TYPE;
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PeerAddressDBItem)
		{
			PeerAddressDBItem other = (PeerAddressDBItem) obj;
			if(other.item.length != item.length)
				return false;
			for(int i=0;i<item.length-2;i++)
				if(other.item[i] != item[i])
					return false;
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(Arrays.copyOf(item, item.length-2));
	}

	public String toString() {
		return super.toString()+" addr:"+new InetSocketAddress(getAddressAsString(), getPort())+" seed:"+seed;
	}
	
	public int getPort() {
		if (item.length == DHTtype.IPV4_DHT.ADDRESS_ENTRY_LENGTH)
			return (item[4] & 0xFF) << 8 | (item[5] & 0xFF);
		if (item.length == DHTtype.IPV6_DHT.ADDRESS_ENTRY_LENGTH)
			return (item[16] & 0xFF) << 8 | (item[17] & 0xFF);
		return 0;
	}
	
	public boolean isSeed() {
		return seed;
	}
	
}
