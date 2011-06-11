package lbms.plugins.mldht.indexer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import lbms.plugins.mldht.utils.Selectable;

public class MetaDataConnectionServer implements Selectable {
	
	public static final int DEFAULT_PORT = 49002;

	NIOConnectionManager conHandler;
	ServerSocketChannel channel;
	InetAddress addr;
	int port = DEFAULT_PORT;
	
	IncomingConnectionHandler connectionHandler;
	
	
	public MetaDataConnectionServer(Class<? extends InetAddress> addressType) {
		addr = AddressUtils.getAvailableAddrs(true, addressType).peek();
		
		try
		{
			channel = ServerSocketChannel.open();
			channel.configureBlocking(false);
		} catch (IOException e)
		{
			DHT.log(e, LogLevel.Error);
		}
	}
	
	public SelectableChannel getChannel() {
		return channel;
	}
	
	public void registrationEvent(NIOConnectionManager manager, SelectionKey key) throws IOException {
		conHandler = manager;
		channel.socket().bind(new InetSocketAddress(addr, port), 100);
		conHandler.setSelection(this, SelectionKey.OP_ACCEPT, true);
	}
	
	public void selectionEvent(SelectionKey key) throws IOException {
		if(key.isAcceptable())
		{
			while(true)
			{
				SocketChannel chan = channel.accept();
				if(chan == null)
					break;
				if(!connectionHandler.canAccept())
				{
					chan.close();
					continue;
				}
				connectionHandler.acceptedConnection(chan);
			}

		}
	}
	
	public void doStateChecks(long now) throws IOException {
		// TODO Auto-generated method stub
	}

	public static interface IncomingConnectionHandler {
		
		public void acceptedConnection(SocketChannel chan);
		
		public boolean canAccept();
		
	}
	
	
}
