package lbms.plugins.mldht.indexer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import lbms.plugins.mldht.indexer.MetaDataGatherer.FetchTask;
import lbms.plugins.mldht.indexer.PullMetaDataConnection.MetaConnectionHandler;
import lbms.plugins.mldht.indexer.assemblyline.AssemblyTask;
import lbms.plugins.mldht.kad.PeerAddressDBItem;

public class TorrentFetcher implements AssemblyTask {
	
	LinkedBlockingQueue<FetchTask> tasks;
	AtomicInteger activeOutgoingConnections;
	MetaDataGatherer container;
	
	public TorrentFetcher(MetaDataGatherer container) {
		this.container = container;
		this.tasks = container.toFetchLink;
		activeOutgoingConnections = container.activeIncomingConnections;
		
	}
	
	
	public boolean performTask() {
		if(activeOutgoingConnections.get() >= container.getNumVirtualNodes() * MetaDataGatherer.MAX_CONCURRENT_METADATA_CONNECTIONS_PER_NODE)
			return false;
		FetchTask task = tasks.poll();
		if(task == null)
			return false;
		fetchMetadata(task);
		return true;
	}
	
	
	private void fetchMetadata(final FetchTask task) {
		PeerAddressDBItem item = task.addresses.get(0);
		task.addresses.remove(item);

		InetSocketAddress addr = new InetSocketAddress(item.getInetAddress(),item.getPort());

		final PullMetaDataConnection conn = new PullMetaDataConnection(task.entry.info_hash, addr);
		if(task.previousConnection != null)
			conn.resume(task.previousConnection);

		conn.metaHandler = new MetaConnectionHandler() {
			public void onTerminate(boolean wasConnected) {
				if(!conn.isState(PullMetaDataConnection.STATE_METADATA_VERIFIED))
				{
					if(task.addresses.size() > 0)
					{
						MetaDataGatherer.log("failed metadata connection and requeueing for "+task.hash);
						task.previousConnection = conn;
						fetchMetadata(task);
					} else {
						MetaDataGatherer.log("failed metadata connection and finished for "+task.hash);
						container.terminatedTasks.add(container.createFailedTask(task.hash));
						performTask();
					}
				} else
				{
					try
					{
						container.writeTorrentFile(conn, task);
						container.terminatedTasks.add(container.createSuccessfulTask(task));
					} catch (IOException e)
					{
						e.printStackTrace();
						MetaDataGatherer.log("successful metadata connection but failed to store for "+task.hash);
						container.terminatedTasks.add(container.createFailedTask(task.hash));
					}
				}

				if(wasConnected)
					activeOutgoingConnections.decrementAndGet();
			}
			
			public void onConnect() {
				activeOutgoingConnections.incrementAndGet();
			}
		};
		// connect after registering the handler in case we get an immediate terminate event
		container.connectionManager.register(conn);
		MetaDataGatherer.log("starting metadata connection for "+task.hash);
	}

	
	
}
