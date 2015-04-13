package the8472.mldht.cli.commands;

import static the8472.bencode.Utils.buf2str;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import the8472.bt.PullMetaDataConnection;
import the8472.bt.PullMetaDataConnection.MetaConnectionHandler;
import the8472.bt.TorrentUtils;
import the8472.mldht.cli.CommandProcessor;

public class GetTorrent extends CommandProcessor {
	
	NIOConnectionManager conMan;
	
	ScheduledThreadPoolExecutor timer;

	Key targetKey;
	
	Set<InetSocketAddress> known = Collections.newSetFromMap(new ConcurrentHashMap<>()) ;
	Queue<InetSocketAddress> canidates = new ConcurrentLinkedQueue<>();
	AtomicInteger activeOrPendingTasks = new AtomicInteger();
	AtomicInteger activeOrPendingConnections = new AtomicInteger();
	AtomicInteger activeConnections = new AtomicInteger();
	
	@Override
	protected void process() {
		String hash = buf2str(ByteBuffer.wrap(arguments.get(0)));
		targetKey = new Key(hash);
		
		
		startLookups();
		scheduleConnections();
	}
	
	void startLookups() {
		dhts.stream().filter(DHT::isRunning).forEach(d -> {
			Optional.ofNullable(d.getServerManager().getRandomActiveServer(false)).ifPresent(srv -> {
				PeerLookupTask task = new PeerLookupTask(srv, d.getNode(), targetKey);
				
				task.setNoAnnounce(true);
				task.setResultHandler(this::addCandidate);
				task.addListener(t -> activeOrPendingTasks.decrementAndGet());
				
				d.getTaskManager().addTask(task);
				activeOrPendingTasks.incrementAndGet();
			});
		});
	}
	
	
	void addCandidate(PeerAddressDBItem toAdd) {
		addCandidate(toAdd.toSocketAddress());
	}
	
	void addCandidate(InetSocketAddress addr) {
		if(known.add(addr))
			canidates.add(addr);
	}
	
	void scheduleConnections() {
		conMan = new NIOConnectionManager("metadata connection handler: "+targetKey);
		timer = new ScheduledThreadPoolExecutor(1);
		timer.setKeepAliveTime(4, TimeUnit.SECONDS);
		timer.allowCoreThreadTimeOut(true);
		timer.scheduleWithFixedDelay(this::startNext, 0, 3, TimeUnit.SECONDS);
	}
	
	void startNext() {
		if(!isRunning()) {
			timer.shutdown();
			return;
		}
			
		if(activeConnections.get() > 10)
			return;
		
		if(activeOrPendingTasks.get() == 0 && activeOrPendingConnections.get() == 0 && canidates.isEmpty()) {
			printErr("no further peers to query\n");
			exit(1);
			return;
		}
		
		Stream.generate(canidates::poll).limit(5).filter(Objects::nonNull).forEach(addr -> {

			println("attempting to fetch from "+addr);

			PullMetaDataConnection con = new PullMetaDataConnection(targetKey.getHash(), addr);
			con.dhtPort = dhts.stream().mapToInt(d -> d.getConfig().getListeningPort()).findAny().getAsInt();
			con.pexConsumer = (toAdd) -> {
				toAdd.forEach(this::addCandidate);
			};
			con.setListener(new MetaConnectionHandler() {

				@Override
				public void onTerminate(boolean wasConnected) {
					if(wasConnected)
						activeConnections.decrementAndGet();
					activeOrPendingConnections.decrementAndGet();
					if(con.isState(PullMetaDataConnection.STATE_METADATA_VERIFIED)) {
						Path torrentName = currentWorkDir.resolve(targetKey.toString(false) +".torrent");
						try(ByteChannel chan = Files.newByteChannel(torrentName, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
							ByteBuffer torrent = TorrentUtils.wrapBareInfoDictionary(con.getMetaData());
							
							Optional<String> name = TorrentUtils.getTorrentName(torrent);
							
							chan.write(torrent);
							name.ifPresent(str -> {
								println("torrent name: "+ str);
							});
							println("written meta to "+torrentName);
							exit(0);
						} catch (IOException e) {
							handleException(e);
							exit(1);
						}
						return;
					}
				}

				@Override
				public void onConnect() {
					activeConnections.incrementAndGet();
				}
			});
			conMan.register(con);
			activeOrPendingConnections.incrementAndGet();
		});
		
		
	}

	

}
