package the8472.mldht;

import static the8472.utils.Functional.tap;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
	
	public int openConnections() {
		return openConnections.get();
	}
	
	public int socketcount() {
		return socketsIncludingHalfOpen.get();
	}
	
	public enum FetchState {
		PENDING,
		SUCCESS,
		FAILURE;
	}
	
	public class FetchTask {
		
		Key hash;
		Instant startTime;
		CompletableFuture<FetchTask> future = new CompletableFuture<>();
		Set<InetSocketAddress> pinged = Collections.newSetFromMap(new ConcurrentHashMap<>()) ;
		Set<InetSocketAddress> connectionAttempted = Collections.newSetFromMap(new ConcurrentHashMap<>());
		Queue<InetSocketAddress> canidates = new ConcurrentLinkedQueue<>();
		Queue<InetSocketAddress> priorityCanidates = new ConcurrentLinkedQueue<>();
		boolean running = true;
		ByteBuffer result;
		AtomicInteger thingsBlockingCompletion = new AtomicInteger();
		
		Set<PullMetaDataConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());
		Map<Integer, MetadataPool> pools = new ConcurrentHashMap<>();
		
		FetchState state = FetchState.PENDING;
		
		public CompletionStage<FetchTask> awaitCompletion() {
			return future;
		}
		
		public Key infohash() {
			return hash;
		}
		
		@Override
		public String toString() {
			String[] str = {
					hash.toString(false),
					"age:",
					Duration.between(startTime, Instant.now()).toString(),
					"con tried:",
					String.valueOf(connectionAttempted.size()),
					"con active:",
					connections.stream().collect(Collectors.groupingBy(PullMetaDataConnection::getState, Collectors.counting())).toString()
			};
			
			return String.join(" ", str);
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
			startTime = Instant.now();
			lookups();
			timer.schedule(this::connections, 1, TimeUnit.SECONDS);
		}
		
		void addCandidate(PeerAddressDBItem toAdd) {
			addCandidate(toAdd.toSocketAddress());
		}
		
		void addCandidate(InetSocketAddress addr) {
			canidates.add(addr);
		}
		
		MetadataPool getPool(int length) {
			return pools.computeIfAbsent(length, l -> {
				return new MetadataPool(l);
			});
		}
		
		Consumer<PeerLookupTask> conf;
		
		public void configureLookup(Consumer<PeerLookupTask> conf) {
			this.conf = conf;
		}
		
		void lookups() {
			List<Runnable> starters = dhts.stream().filter(DHT::isRunning).map(d -> {
				return Optional.ofNullable(d.getServerManager().getRandomActiveServer(false)).map(srv -> {
					PeerLookupTask task = new PeerLookupTask(srv, d.getNode(), hash);
					
					task.setNoAnnounce(true);
					if(conf != null)
						conf.accept(task);
					task.setResultHandler(this::addCandidate);
					task.addListener(t -> thingsBlockingCompletion.decrementAndGet());
					
					thingsBlockingCompletion.incrementAndGet();
					
					
					
					future.thenAccept(x -> task.kill());

					return (Runnable)() -> {d.getTaskManager().addTask(task);};
				});
			}).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());

			// start tasks after we've incremented counters for all tasks
			// otherwise tasks might finish so fast that the counters go back down to 0
			starters.forEach(Runnable::run);
		}
		
		AtomicInteger pings = new AtomicInteger(0);
		
		void doPings() {
			for(InetSocketAddress addr : canidates) {
				if(pings.get() > 10)
					break;
				if(!pinged.add(addr) || connectionAttempted.contains(addr))
					continue;
				
				dhts.stream().filter(d -> d.getType().PREFERRED_ADDRESS_TYPE.isInstance(addr.getAddress())).findAny().ifPresent(d -> {
					RPCServer srv = d.getServerManager().getRandomActiveServer(false);
					if(srv == null)
						return;
					PingRequest req = new PingRequest();
					req.setDestination(addr);
					RPCCall call = new RPCCall(req);
					call.addListener(new RPCCallListener() {
						
						@Override
						public void onTimeout(RPCCall c) {
							pings.decrementAndGet();
						}
												
						@Override
						public void onStall(RPCCall c) {}
						
						@Override
						public void onResponse(RPCCall c, MessageBase rsp) {
							pings.decrementAndGet();
							priorityCanidates.add(addr);
						}
					});
					pings.incrementAndGet();
					srv.doCall(call);
				});
								
			}
			
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
			
			if(openConnections.get() > maxOpen || socketsIncludingHalfOpen.get() > maxSockets) {
				doPings();
				return;
			}
			
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
		return fetch(infohash, null);
	}
	
	public FetchTask fetch(Key infohash, Consumer<FetchTask> configure) {
		FetchTask t = new FetchTask();
		t.hash = infohash;
		if(configure != null)
			configure.accept(t);
		t.start();
		
		return t;
	}
	
	
	
	
	

}
