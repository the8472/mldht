package the8472.mldht;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.PeerAddressDBItem;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.tasks.PeerLookupTask;
import lbms.plugins.mldht.kad.utils.AddressUtils;
import lbms.plugins.mldht.utils.NIOConnectionManager;
import the8472.bt.MetadataPool;
import the8472.bt.MetadataPool.Completion;
import the8472.bt.PullMetaDataConnection;
import the8472.bt.PullMetaDataConnection.CONNECTION_STATE;
import the8472.bt.PullMetaDataConnection.MetaConnectionHandler;
import the8472.bt.UselessPeerFilter;
import the8472.utils.concurrent.LoggingScheduledThreadPoolExecutor;

public class TorrentFetcher {
	
	Collection<DHT> dhts;
	ScheduledThreadPoolExecutor timer;
	NIOConnectionManager conMan = new NIOConnectionManager("torrent fetcher");
	
	AtomicInteger socketsIncludingHalfOpen = new AtomicInteger();
	AtomicInteger openConnections = new AtomicInteger();
	
	List<FetchTask> tasks = new ArrayList<>();
	
	int maxOpen = 10;
	int maxSockets = 1000;
	
	public TorrentFetcher(Collection<DHT> dhts) {
		this.dhts = dhts;
		timer = new LoggingScheduledThreadPoolExecutor(1, LoggingScheduledThreadPoolExecutor.namedDaemonFactory("TorrentFetcher Timer"), t -> DHT.log(t, LogLevel.Fatal));
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
	
	boolean socketLimitsReached() {
		return openConnections.get() > maxOpen || socketsIncludingHalfOpen.get() > maxSockets;
	}
	
	UselessPeerFilter pf;
	
	public void setPeerFilter(UselessPeerFilter pf) {
		this.pf = pf;
	}
	
	ScheduledFuture<?> f = null;
	
	void ensureRunning() {
		synchronized (this) {
			if(f == null && tasks.size() > 0) {
				f = timer.scheduleWithFixedDelay(this::scheduleConnections, 0, 1, TimeUnit.SECONDS);
			}
				
		}
	}
	
	void scheduleConnections() {
		synchronized (this) {
			if(tasks.size() == 0 && f != null) {
				f.cancel(false);
				f = null;
				return;
			}
			
			int offset = ThreadLocalRandom.current().nextInt(tasks.size());
				
			for(int i= 0;i<tasks.size();i++) {
				int idx = Math.floorMod(i+offset, tasks.size());
				
				if(socketLimitsReached())
					break;
				
				tasks.get(idx).connections();
			}
			
			
				
						
		}
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
		Map<InetSocketAddress, PullMetaDataConnection.CONNECTION_STATE> closed = new ConcurrentHashMap<>();
		ConcurrentHashMap<InetSocketAddress, Set<InetAddress>> candidates = new ConcurrentHashMap<>();
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
					connections.stream().collect(Collectors.groupingBy(PullMetaDataConnection::getState, Collectors.counting())).toString(),
					"con closed:",
					closeCounts().toString()
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
			remove(this);
			future.complete(this);
		}
		
		public Map<CONNECTION_STATE, Long> closeCounts() {
			return closed.entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.counting()));
		}
		
		public int attemptedCount() {
			return connectionAttempted.size();
		}

		void start() {
			startTime = Instant.now();
			lookups();
		}
		
		void addCandidate(KBucketEntry source, PeerAddressDBItem toAdd) {
			addCandidate(source.getAddress().getAddress() ,toAdd.toSocketAddress());
		}
		
		void addCandidate(InetAddress source, InetSocketAddress toAdd) {
			
			if(pf != null && pf.isBad(toAdd))
				return;
			
			candidates.compute(toAdd, (k, sources) -> {
				Set<InetAddress> newSources = new HashSet<>();
				if(source != null)
					newSources.add(source);
				if(sources != null)
					newSources.addAll(sources);
				return newSources;
			});
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
		
		void connections() {
			if(!running) {
				return;
			}
			
			if(thingsBlockingCompletion.get() == 0 && candidates.isEmpty()) {
				stop();
				return;
			}
			
			candidates.keySet().removeAll(connectionAttempted);
			
			Comparator<Map.Entry<InetSocketAddress, Set<InetAddress>>> comp = Map.Entry.comparingByValue(Comparator.comparingInt(Set::size));
			comp = comp.reversed();
			// deprioritize teredo addresses
			comp = comp.thenComparing(Map.Entry.comparingByKey(Comparator.comparingInt((InetSocketAddress addr) -> AddressUtils.isTeredo(addr.getAddress()) ? 1 : 0))) ;
			
			InetSocketAddress[] cands = candidates.entrySet().stream().sorted(comp).map(Map.Entry::getKey).toArray(InetSocketAddress[]::new);
			
			int i = 0;
			
			for(InetSocketAddress addr : cands) {
				
				if(socketLimitsReached())
					break;
				if(i++ > 5)
					break;
				
				connectionAttempted.add(addr);
				candidates.remove(addr);

				PullMetaDataConnection con = new PullMetaDataConnection(hash.getHash(), addr);
				
				con.keepPexOnlyOpen(cands.length < 20);
				
				con.poolGenerator = this::getPool;
				con.dhtPort = dhts.stream().mapToInt(d -> d.getConfig().getListeningPort()).findAny().getAsInt();
				con.pexConsumer = (toAdd) -> {
					toAdd.forEach(item -> {
						this.addCandidate(addr.getAddress(), item);
					});
				};
				
				connections.add(con);
				
				con.setListener(new MetaConnectionHandler() {

					@Override
					public void onTerminate() {
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
						if(pf != null)
							pf.insert(con);
					}
					
					public void onStateChange(CONNECTION_STATE oldState, CONNECTION_STATE newState) {
						if(newState == CONNECTION_STATE.STATE_CLOSED) {
							closed.put(addr, oldState);
							socketsIncludingHalfOpen.decrementAndGet();
						}
							
						if(oldState == CONNECTION_STATE.STATE_CONNECTING && newState != CONNECTION_STATE.STATE_CLOSED)
							openConnections.incrementAndGet();
						if(oldState != CONNECTION_STATE.STATE_INITIAL && oldState != CONNECTION_STATE.STATE_CONNECTING && newState == CONNECTION_STATE.STATE_CLOSED)
							openConnections.decrementAndGet();
					};

					@Override
					public void onConnect() {
						
					}
				});
				conMan.register(con);
				thingsBlockingCompletion.incrementAndGet();
				socketsIncludingHalfOpen.incrementAndGet();
			}
		}
		
	}
	
	void remove(FetchTask t) {
		synchronized (this) {
			tasks.remove(t);
		}
	}
	
	void add(FetchTask t) {
		synchronized (this) {
			tasks.add(t);
		}
		ensureRunning();
	}
	
	public FetchTask fetch(Key infohash) {
		return fetch(infohash, null);
	}
	
	public FetchTask fetch(Key infohash, Consumer<FetchTask> configure) {
		FetchTask t = new FetchTask();
		t.hash = infohash;
		if(configure != null)
			configure.accept(t);
		add(t);
		t.start();
		
		return t;
	}
	
	
	
	
	

}
