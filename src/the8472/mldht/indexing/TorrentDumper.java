package the8472.mldht.indexing;

import static the8472.utils.Functional.typedGet;

import the8472.bencode.BDecoder;
import the8472.bencode.BEncoder;
import the8472.bt.TorrentUtils;
import the8472.mldht.Component;
import the8472.mldht.TorrentFetcher;
import the8472.mldht.TorrentFetcher.FetchTask;
import the8472.utils.ConfigReader;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.messages.AnnounceRequest;
import lbms.plugins.mldht.kad.messages.GetPeersRequest;
import lbms.plugins.mldht.kad.messages.MessageBase;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class TorrentDumper implements Component {
	
	Collection<DHT> dhts;
	Path storageDir = Paths.get(".", "dump-storage");
	Path statsDir = storageDir.resolve("stats");
	Path torrentDir = storageDir.resolve("torrents");
	
	ScheduledThreadPoolExecutor scheduler;
	
	ConcurrentSkipListMap<Key, FetchStats> fromMessages;
	
	TorrentFetcher fetcher;
	
	static class FetchStats {
		Key k;

		public FetchStats(Key k) {
			Objects.requireNonNull(k);
			this.k = k;
		}
		
		static FetchStats fromBencoded(Map<String, Object> map) {
			Key k = typedGet(map, "k", byte[].class).map(Key::new).orElseThrow(() -> new IllegalArgumentException("missing key in serialized form"));
			return new FetchStats(k);
		}
		
		Map<String, Object> forBencoding() {
			Map<String, Object> map = new TreeMap<>();
			
			map.put("k", k.getHash());
			
			return map;
		}

		public Key getK() {
			return k;
		}
		
		// TODO: implement merge. not merging -> ultra-dumb fetcher
		public FetchStats merge(FetchStats other) {
			if(!k.equals(other.k))
				throw new IllegalArgumentException("key mismatch");
			return other;
		}
		
		public Path name(Path dir, String suffix) {
			return dir.resolve(k.toString(false)+suffix);
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
		});
		fetcher = new TorrentFetcher(dhts);

		dhts.forEach(d -> d.addIncomingMessageListener(this::incomingMessage));
		try {
			Files.createDirectories(statsDir);
			Files.createDirectories(torrentDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		scheduler.scheduleWithFixedDelay(this::dumpStats, 10, 1, TimeUnit.SECONDS);
		scheduler.scheduleWithFixedDelay(this::startFetches, 10, 1, TimeUnit.SECONDS);
		
		
	}
	
	void incomingMessage(DHT d, MessageBase m) {
		if(m instanceof GetPeersRequest) {
			GetPeersRequest gpr = (GetPeersRequest) m;
			process(gpr.getInfoHash(), gpr.getOrigin().getAddress(), null);
		}
		if(m instanceof AnnounceRequest) {
			AnnounceRequest anr = (AnnounceRequest) m;
			process(anr.getInfoHash(), anr.getOrigin().getAddress(), anr.getNameUTF8().orElse(null));
		}
	}
	
	void process(Key k, InetAddress src, String name) {
		
		
		fromMessages.compute(k, (unused, f) -> {
			FetchStats f2 = new FetchStats(k);
			return f == null ? f2 : f.merge(f2);
		});
						
	}
	
	Key cursor = Key.MIN_KEY;
	
	void dumpStats() {
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
			
			Path statsName = s.name(statsDir, ".stats");

			try {

				if(Files.exists(statsName)) {
					s = FetchStats.fromBencoded(new BDecoder().decode(ByteBuffer.wrap(Files.readAllBytes(statsName)))).merge(s);
				}

				// TODO: atomic-move
				try(FileChannel ch = FileChannel.open(statsName, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
					ByteBuffer buf = new BEncoder().encode(s.forBencoding(), 16*1024);
					ch.write(buf);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}
				
	}
	
	void startFetches() {
		
		Key start = Key.createRandomKey();
		
		try {
			Files.find(statsDir, 3, (p, attr) -> {
				if(!attr.isRegularFile())
					return false;
				String name = p.getFileName().toString();
				return name.matches("[0-9A-F]{40}.stats") && name.compareTo(start.toString(false)) > 0;
			}).forEach(p -> {
				try {
					fetch(p, FetchStats.fromBencoded(new BDecoder().decode(ByteBuffer.wrap(Files.readAllBytes(p)))));
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	AtomicInteger activeCount = new AtomicInteger();
	ConcurrentHashMap<Key, FetchTask> activeTasks = new ConcurrentHashMap<>();
	
	void fetch(Path statsFile, FetchStats stats) {
		Key k = stats.getK();
		
		if(activeTasks.containsKey(k))
			return;
		
		if(activeCount.get() > 100)
			return;
		
		FetchTask t = fetcher.fetch(k);
		
		activeCount.incrementAndGet();
		activeTasks.put(k, t);
		
		t.awaitCompletion().thenRun(() -> {
			scheduler.execute(() -> {
				// run on the scheduler so we don't end up with interfering file ops
				taskFinished(statsFile, stats, t);
			});
			
		});
	}
	
	void taskFinished(Path statsFile, FetchStats stats, FetchTask t) {
		activeCount.decrementAndGet();
		activeTasks.remove(t.infohash());
		try {
			Files.delete(statsFile);
			if(!t.getResult().isPresent())
				return;
			ByteBuffer buf = t.getResult().get();
			try(FileChannel chan = FileChannel.open(stats.name(torrentDir, ".torrent"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
				chan.write(TorrentUtils.wrapBareInfoDictionary(buf));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		scheduler.shutdown();
		activeTasks.values().forEach(FetchTask::stop);
	}

}
