package the8472.mldht.indexing;

import static java.lang.Math.min;
import static the8472.utils.Functional.typedGet;

import the8472.bencode.BDecoder;
import the8472.bencode.BEncoder;
import the8472.bt.TorrentUtils;
import the8472.mldht.Component;
import the8472.mldht.TorrentFetcher;
import the8472.mldht.TorrentFetcher.FetchTask;
import the8472.mldht.indexing.TorrentDumper.FetchStats.State;
import the8472.utils.ConfigReader;
import the8472.utils.io.FileIO;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.RPCServer;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Stream;


public class TorrentDumper implements Component {
	
	Collection<DHT> dhts;
	Path storageDir = Paths.get(".", "dump-storage");
	Path statsDir = storageDir.resolve("stats");
	Path torrentDir = storageDir.resolve("torrents");
	
	ScheduledThreadPoolExecutor scheduler;
	
	ConcurrentSkipListMap<Key, FetchStats> fromMessages;
	ConcurrentMap<InetAddress, Long> blocklist = new ConcurrentHashMap<>();
	
	TorrentFetcher fetcher;
	
	static class FetchStats {
		final Key k;
		int insertCount = 1;
		InetAddress lastTouchedBy;
		long creationTime = -1;
		long lastFetchTime = -1;
		State state = State.INITIAL;
		
		enum State {
			INITIAL,
			PRIORITY,
			FAILED;
			
			public Path stateDir(Path statsdir) {
				return statsdir.resolve(name().toLowerCase());
			}
			
			
		}

		public FetchStats(Key k, Consumer<FetchStats> init) {
			Objects.requireNonNull(k);
			this.k = k;
			if(init != null)
				init.accept(this);
		}

		static FetchStats fromBencoded(Map<String, Object> map) {
			Key k = typedGet(map, "k", byte[].class).map(Key::new).orElseThrow(() -> new IllegalArgumentException("missing key in serialized form"));
			
			return new FetchStats(k, fs -> {
				typedGet(map, "addr", byte[].class).map(t -> {
					try {
						return InetAddress.getByAddress(t);
					} catch (UnknownHostException e) {
						return null;
					}
				}).ifPresent(addr -> fs.lastTouchedBy = addr);
				
				typedGet(map, "state", byte[].class).map(b -> new String(b, StandardCharsets.ISO_8859_1)).map(str -> {
					try {
						return State.valueOf(str);
					} catch (IllegalArgumentException e) {
						return null;
					}
				}).ifPresent(st -> fs.state = st);
				
				typedGet(map, "created", Long.class).ifPresent(time -> fs.creationTime = time);
				typedGet(map, "cnt", Long.class).ifPresent(cnt -> fs.insertCount = cnt.intValue());
				typedGet(map, "fetchtime", Long.class).ifPresent(time -> fs.lastFetchTime = time);
				
			});
		}
		
		Map<String, Object> forBencoding() {
			Map<String, Object> map = new TreeMap<>();
			
			map.put("k", k.getHash());
			map.put("cnt", insertCount);
			map.put("addr", lastTouchedBy.getAddress());
			map.put("created", creationTime);
			map.put("state", state.name());
			map.put("fetchtime", lastFetchTime);
			
			return map;
		}

		public Key getK() {
			return k;
		}
		
		public FetchStats merge(FetchStats other) {
			if(!k.equals(other.k))
				throw new IllegalArgumentException("key mismatch");
			
			boolean otherIsNewer = other.creationTime > creationTime;
			
			insertCount += other.insertCount;
			lastTouchedBy = otherIsNewer ? other.lastTouchedBy : lastTouchedBy;
			creationTime = min(creationTime, other.creationTime);
			state = otherIsNewer ? other.state : state;
			
			return this;
		}
		
		public void setState(State newState) {
			state = newState;
		}
		
		public Path name(Path dir, String suffix) {
			return dir.resolve(k.toString(false)+suffix);
		}
		
		public Path statsName(Path statsDir, State st) {
			if(st == null)
				st = state;
			return name(st.stateDir(statsDir), ".stats");
			
		}
		
		
	}

	@Override
	public void start(Collection<DHT> dhts, ConfigReader config) {
		this.dhts = dhts;
		fromMessages = new ConcurrentSkipListMap<>();
		scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
					
					@Override
					public void uncaughtException(Thread t, Throwable e) {
						e.printStackTrace();
					}
				});
				
				t.setDaemon(true);

				return t;
			}
		}) {
			@Override
			protected void afterExecute(Runnable r, Throwable t) {
				super.afterExecute(r, t);
				
				if(t == null && r instanceof FutureTask<?>) {
					FutureTask<?> ft = (FutureTask<?>) r;
					if(ft.isDone() && !ft.isCancelled()) {
						try {
							ft.get();
						} catch (InterruptedException | ExecutionException e) {
							t = e.getCause();
						}
					}
				}
				
				if(t != null)
					log(t);
			}
		};
		fetcher = new TorrentFetcher(dhts);
		fetcher.setMaxOpen(40);

		dhts.forEach(d -> d.addIncomingMessageListener(this::incomingMessage));
		try {
			Files.createDirectories(torrentDir);
			for(State st : FetchStats.State.values()) {
				Files.createDirectories(st.stateDir(statsDir));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		scheduler.scheduleWithFixedDelay(this::dumpStats, 10, 1, TimeUnit.SECONDS);
		scheduler.scheduleWithFixedDelay(this::startFetches, 10, 1, TimeUnit.SECONDS);
		scheduler.scheduleWithFixedDelay(this::cleanBlocklist, 1, 1, TimeUnit.MINUTES);
		scheduler.scheduleWithFixedDelay(this::diagnostics, 30, 30, TimeUnit.SECONDS);
		scheduler.scheduleWithFixedDelay(this::purgeStats, 5, 30, TimeUnit.MINUTES);
		
	}
	
	void log(Throwable t) {
		DHT.log(t, LogLevel.Error);
	}
	
	void cleanBlocklist() {
		long now = System.currentTimeMillis();
		blocklist.entrySet().removeIf(e -> {
			return (now - e.getValue()) > TimeUnit.MINUTES.toMillis(10);
		});
		
	}
	
	void incomingMessage(DHT d, MessageBase m) {
		if(m instanceof GetPeersRequest) {
			GetPeersRequest gpr = (GetPeersRequest) m;
			
			RPCServer srv = m.getServer();
			
			Key theirID = gpr.getID();
			
			if(d.getNode().isLocalId(theirID))
				return;
			
			Key ourId = srv.getDerivedID();
			Key target = gpr.getInfoHash();
			int myCloseness = ourId.distance(target).leadingOneBit();
			int theirCloseness = theirID.distance(target).leadingOneBit();
			
			if(theirCloseness == -1)
				return;  // they're looking exactly for their own id
			if(theirCloseness > myCloseness && theirCloseness - myCloseness >= 8)
				return;  // they're looking for something that's significantly closer to their own ID than we are
			
			if(m.getID().equals(((GetPeersRequest) m).getInfoHash()))
				return;
			process(gpr.getInfoHash(), gpr.getOrigin().getAddress(), null);
		}
		if(m instanceof AnnounceRequest) {
			AnnounceRequest anr = (AnnounceRequest) m;
			process(anr.getInfoHash(), anr.getOrigin().getAddress(), anr.getNameUTF8().orElse(null));
		}
	}
	
	void process(Key k, InetAddress src, String name) {
		
		
		fromMessages.compute(k, (unused, f) -> {
			FetchStats f2 = new FetchStats(k, init -> {
				init.lastTouchedBy = src;
				init.insertCount = 1;
				init.creationTime = System.currentTimeMillis();
			});
			return f == null ? f2 : f.merge(f2);
		});
						
	}
	
	Key cursor = Key.MIN_KEY;
	
	void dumpStats() {
		long now = System.currentTimeMillis();
		
		for(;;) {
			Entry<Key, FetchStats> entry = fromMessages.ceilingEntry(cursor);
			if(entry == null) {
				cursor = Key.MIN_KEY;
				break;
			}
			
			Key k = entry.getKey();
			FetchStats s = entry.getValue();
			
			fromMessages.remove(k);
			
			cursor = k.add(Key.setBit(159));

			
			if(Files.exists(s.name(torrentDir, ".torrent"))) {
				continue;
			}
			


			
			try {
				
				Optional<Path> existing = Stream.of(s.statsName(statsDir, FetchStats.State.FAILED), s.statsName(statsDir, FetchStats.State.PRIORITY), s.statsName(statsDir, FetchStats.State.INITIAL)).filter(Files::isRegularFile).findFirst();

				if(!existing.isPresent()) {
					// only throttle IPs for new hashes we don't already know about and wouldn't try anyway
					if(activeCount.get() > 50 && blocklist.putIfAbsent(s.lastTouchedBy, now) != null)
						continue;
				}
				
				if(existing.isPresent()) {
					Path p = existing.get();
					try {
						FetchStats old = FetchStats.fromBencoded(new BDecoder().decode(ByteBuffer.wrap(Files.readAllBytes(p))));
						
						// avoid double-taps
						if(old.lastTouchedBy.equals(s.lastTouchedBy))
							return;
						
						s.merge(old);
						
					} catch (IOException e) {
						log(e);
					}
				}
				
				if(s.state == State.INITIAL && s.insertCount > 1) {
					s.state = State.PRIORITY;
					if(existing.isPresent())
						Files.delete(existing.get());
				}
					
				

				// TODO: atomic-move
				try(FileChannel ch = FileChannel.open(s.statsName(statsDir, null), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
					ByteBuffer buf = new BEncoder().encode(s.forBencoding(), 16*1024);
					ch.write(buf);
				}
			} catch (Exception e) {
				log(e);
			}
			
			
			
		}
				
	}
	
	void purgeStats() {
		Path failedDir = FetchStats.State.FAILED.stateDir(statsDir);
		
		BDecoder dec = new BDecoder();
		
		long now = System.currentTimeMillis();
		
		try {
			Stream<FetchStats> st = Files.find(failedDir, 1, (p, attr) -> {
				return p.getFileName().toString().matches("\\.stats$");
			}).map(p -> {
				try {
					return FetchStats.fromBencoded(dec.decode(ByteBuffer.wrap(Files.readAllBytes(p))));
				} catch (IOException e) {
					log(e);
					return null;
				}
			});
			
			st.filter(Objects::nonNull).filter(stat -> now - stat.lastFetchTime > TimeUnit.HOURS.toMillis(2)).forEach(stat -> {
				try {
					Files.delete(stat.statsName(statsDir, null));
				} catch (IOException e) {
					log(e);
				}
			});
			
		} catch (IOException e) {
			log(e);
		}
	}
	
	Stream<FetchStats> fetchStatsStream() throws IOException {
		Key start = Key.createRandomKey();
		BDecoder dec = new BDecoder();
		
		BiPredicate<Path, BasicFileAttributes> fileFilter = (p, attr) -> {
			if(!attr.isRegularFile())
				return false;
			String name = p.getFileName().toString();
			return name.matches("^[0-9A-F]{40}\\.stats$") && name.compareTo(start.toString(false)) > 0;
		};
		
		Path prio = FetchStats.State.PRIORITY.stateDir(statsDir);
		Path normal = FetchStats.State.INITIAL.stateDir(statsDir);
		
		return Stream.concat(Files.find(prio, 3, fileFilter), Files.find(normal, 3, fileFilter)).map(p -> {
			try {
				return FetchStats.fromBencoded(dec.decode(ByteBuffer.wrap(Files.readAllBytes(p))));
			} catch (IOException e) {
				log(e);
				return null;
			}
		}).filter(Objects::nonNull);
	}
	
	void startFetches() {
		try {
			fetchStatsStream().forEach(this::fetch);
		} catch (Exception e) {
			log(e);
		}
		
	}
	
	AtomicInteger activeCount = new AtomicInteger();
	ConcurrentHashMap<Key, FetchTask> activeTasks = new ConcurrentHashMap<>();
	
	void fetch(FetchStats stats) {
		Key k = stats.getK();
		
		if(activeTasks.containsKey(k))
			return;
		
		if(activeCount.get() > 100)
			return;
		
		FetchTask t = fetcher.fetch(k, (fetch) -> {
			fetch.configureLookup(lookup -> {
				lookup.setFastTerminate(true);
				lookup.setLowPriority(true);
			});
		});
		
		activeCount.incrementAndGet();
		activeTasks.put(k, t);
		
		t.awaitCompletion().thenRun(() -> {
			scheduler.execute(() -> {
				// run on the scheduler so we don't end up with interfering file ops
				taskFinished(stats, t);
			});
			
		});
	}
	
	void taskFinished(FetchStats stats, FetchTask t) {
		activeCount.decrementAndGet();
		blocklist.remove(stats.lastTouchedBy);
		activeTasks.remove(t.infohash());
		try {
			Path statsFile = stats.statsName(statsDir, null);
			if(Files.isRegularFile(statsFile))
				Files.delete(statsFile);
			
			if(!t.getResult().isPresent()) {
				stats.setState(FetchStats.State.FAILED);
				stats.lastFetchTime = System.currentTimeMillis();
				try(FileChannel statsChan = FileChannel.open(stats.statsName(statsDir, null), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
					statsChan.write(new BEncoder().encode(stats.forBencoding(), 4*1024));
				}
				return;
			}
			ByteBuffer buf = t.getResult().get();
			try(FileChannel chan = FileChannel.open(stats.name(torrentDir, ".torrent"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
				chan.write(TorrentUtils.wrapBareInfoDictionary(buf));
			}
		} catch (Exception e) {
			log(e);
		}
		
	}
	
	void diagnostics() {
		try {
			FileIO.writeAndAtomicMove(storageDir.resolve("dumper.log"), (p) -> {
				p.format("Fetcher:%n established: %d%n sockets: %d %n%n", fetcher.openConnections(), fetcher.socketcount());
				
				p.println("FetchTasks");
				activeTasks.values().forEach(ft -> {
					p.println(ft.toString());
				});
			});
		} catch (IOException e) {
			log(e);
		}
	}
	

	@Override
	public void stop() {
		scheduler.shutdown();
		activeTasks.values().forEach(FetchTask::stop);
	}

}
