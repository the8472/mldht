package lbms.plugins.mldht.kad;

import java.net.InetAddress;

public interface DHTIndexingListener {
	
	public void incomingPeersRequest(Key infoHash, InetAddress sourceAddress, Key nodeID);
	
	
}
