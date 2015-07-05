package the8472.mldht;

import static the8472.utils.Functional.tap;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.RPCCall;
import lbms.plugins.mldht.kad.RPCCallListener;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.messages.MessageBase;
import lbms.plugins.mldht.kad.messages.PingRequest;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import the8472.bt.MetadataPool;
import the8472.bt.MetadataPool.Completion;
import the8472.bt.PullMetaDataConnection;
import the8472.bt.PullMetaDataConnection.MetaConnectionHandler;

public class TorrentFetcher {
	
	Collection<DHT> dhts;
	ScheduledThreadPoolExecutor timer;
	NIOConnectionManager conMan = new NIOConnectionManager("torrent fetcher");
	
	AtomicInteger socketsIncludingHalfOpen = new AtomicInteger();
	AtomicInteger openConnections = new AtomicInteger();
	
	int maxOpen = 10;
	int maxSockets = 1000;
	
	public TorrentFetcher(Collection<DHT> dhts) {
		this.dhts = dhts;
		timer = new ScheduledThreadPoolExecutor(1);
		timer.setThreadFactory((r) -> tap(new Thread(r),t -> {
			t.setName("Torrent Fetcher Timer");
			t.setDaemon(true);
		}));
		timer.setKeepAliveTime(4, TimeUnit.SECONDS);
		timer.allowCoreThreadTimeOut(true);
	}
	
	public void setMaxSockets(int maxHalfOpen) {
		this.maxSockets = maxHalfOpen;
	}
	
	public void setMaxOpen(int maxOpen) {
		this.maxOpen = maxOpen;
	}
	
	public enum FetchState {
		PENDING,
		SUCCESS,
		FAILURE;
	}
	
	public class FetchTask {
		
		Key hash;
		CompletableFuture<FetchTask> future = new CompletableFuture<>();
		Set<InetSocketAddress> pinged = Collections.newSetFromMap(new ConcurrentHashMap<>()) ;
		Set<InetSocketAddress> connectionAttempted = Collections.newSetFromMap(new ConcurrentHashMap<>());
		Queue<InetSocketAddress> canidates = new ConcurrentLinkedQueue<>();
		Queue<InetSocketAddress> priorityCanidates = new ConcurrentLinkedQueue<>();
		boolean running = true;
		ByteBuffer result;
		AtomicInteger thingsBlockingCompletion = new AtomicInteger();
		
		Set<PullMetaDataConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
		Map<Integer, MetadataPool> pools = new ConcurrentHashMap<Integer, MetadataPool>();
		
		FetchState state = FetchState.PENDING;
		
		public CompletionStage<FetchTask> awaitCompletion() {
			return future;
		}
		
		public Key infohash() {
			return hash;
		}

		public FetchState getState() {
			return state;
		}
		
		public Optional<ByteBuffer> getResult() {
			return Optional.ofNullable(result);
		}
		
		public void stop() {
			if(!running)
				return;
			running = false;
			if(state == FetchState.PENDING)
				state = FetchState.FAILURE;
			connections.forEach(c -> {
				try {
					c.terminate("fetch task finished");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			future.complete(this);
		}

		void start() {
			lookups();
			timer.schedule(this::connections, 1, TimeUnit.SECONDS);
		}
		
		void addCandidate(PeerAddressDBItem toAdd) {
			addCandidate(toAdd.toSocketAddress());
		}
		
		void addCandidate(InetSocketAddress addr) {
			if(pinged.add(addr)) {
				dhts.stream().filter(d -> d.getType().PREFERRED_ADDRESS_TYPE.isInstance(addr.getAddress())).findAny().ifPresent(d -> {
					RPCServer srv = d.getServerManager().getRandomActiveServer(false);
					if(srv == null)
						return;
					PingRequest req = new PingRequest();
					req.setDestination(addr);
					RPCCall call = new RPCCall(req);
					call.addListener(new RPCCallListener() {
						
						@Override
						public void onTimeout(RPCCall c) {}
						
						@Override
						public void onStall(RPCCall c) {}
						
						@Override
						public void onResponse(RPCCall c, MessageBase rsp) {
							priorityCanidates.add(addr);
						}
					});
					srv.doCall(call);
				});
				canidates.add(addr);
			}
				
		}
		
		MetadataPool getPool(int length) {
			return pools.computeIfAbsent(length, l -> {
				return new MetadataPool(l);
			});
		}
		
		void lookups() {
			dhts.stream().filter(DHT::isRunning).forEach(d -> {
				Optional.ofNullable(d.getServerManager().getRandomActiveServer(false)).ifPresent(srv -> {
					PeerLookupTask task = new PeerLookupTask(srv, d.getNode(), hash);
					
					task.setNoAnnounce(true);
					task.setResultHandler(this::addCandidate);
					task.addListener(t -> thingsBlockingCompletion.decrementAndGet());
					
					d.getTaskManager().addTask(task);
					thingsBlockingCompletion.incrementAndGet();
				});
			});
		}
		
		void connections() {
			if(!running) {
				return;
			}
			
			timer.schedule(this::connections, 1, TimeUnit.SECONDS);

			if(thingsBlockingCompletion.get() == 0 && canidates.isEmpty()) {
				stop();
				return;
			}
			
			if(openConnections.get() > maxOpen || socketsIncludingHalfOpen.get() > maxSockets)
				return;
			
			Stream.generate(() -> {
				InetSocketAddress addr = priorityCanidates.poll();
				if(addr == null)
					addr = canidates.poll();
				return addr;
			}).filter(a -> !connectionAttempted.contains(a)).limit(5).filter(Objects::nonNull).forEach(addr -> {
				
				connectionAttempted.add(addr);

				PullMetaDataConnection con = new PullMetaDataConnection(hash.getHash(), addr);
				
				con.poolGenerator = this::getPool;
				con.dhtPort = dhts.stream().mapToInt(d -> d.getConfig().getListeningPort()).findAny().getAsInt();
				con.pexConsumer = (toAdd) -> {
					toAdd.forEach(this::addCandidate);
				};
				
				connections.add(con);
				
				con.setListener(new MetaConnectionHandler() {

					@Override
					public void onTerminate(boolean wasConnected) {
						if(wasConnected)
							openConnections.decrementAndGet();
						
						connections.remove(con);
						
						MetadataPool pool = con.getMetaData();
						
						if(pool != null) {
							if(pool.status() == Completion.SUCCESS) {
								result = pool.merge();
								state = FetchState.SUCCESS;
								stop();
							}
							if(pool.status() == Completion.FAILED) {
								pools.remove(pool.bytes(), pool);
							}
						}
							
						thingsBlockingCompletion.decrementAndGet();
						socketsIncludingHalfOpen.decrementAndGet();
					}

					@Override
					public void onConnect() {
						openConnections.incrementAndGet();
					}
				});
				conMan.register(con);
				thingsBlockingCompletion.incrementAndGet();
				socketsIncludingHalfOpen.incrementAndGet();
			});
		}
		
	}
	
	
	public FetchTask fetch(Key infohash) {
		
		FetchTask t = new FetchTask();
		t.hash = infohash;
		t.start();
		
		return t;
	}
	
	
	
	
	

}
