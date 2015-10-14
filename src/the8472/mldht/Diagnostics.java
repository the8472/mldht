package the8472.mldht;

import static the8472.utils.Functional.unchecked;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Database;
import lbms.plugins.mldht.kad.Database.PeersSeeds;
import lbms.plugins.mldht.kad.GenericStorage;
import lbms.plugins.mldht.kad.GenericStorage.StorageItem;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.Node.RoutingTable;
import lbms.plugins.mldht.kad.Prefix;
import the8472.bencode.Utils;
import the8472.utils.Arrays;
import the8472.utils.io.NetMask;

public class Diagnostics {
	
	Collection<DHT> dhts;
	Path logDir;
	
	void init(Collection<DHT> dhts, Path logDir) {
		
		this.dhts = dhts;
		this.logDir = logDir;

		dhts.stream().findAny().ifPresent(d -> d.getScheduler().scheduleWithFixedDelay(this::writeAll, 10, 30, TimeUnit.SECONDS));
	}
	
	void writeAll() {
		try {
			printMain();
			printRoutingTable();
			printDatabases();
			printPUTStorage();
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void printPUTStorage() throws IOException {
		Path file = logDir.resolve("putDB.log");
		
		writeAndAtomicMove(file, writer -> dhts.stream().filter(DHT::isRunning).forEach(d -> {
			writer.append("Type: " + d.getType().shortName + "\n");
			formatStorage(writer, d.getStorage());
		}));
	}
	
	public void formatStorage(Appendable writer, GenericStorage storage) {
		Formatter f = new Formatter(writer);
		
		storage.getItems().forEach((k, v) -> {
			f.format("%s mutable:%b %n",
				k,
				v.mutable()
			);
			f.format("%s%n%n", Utils.stripToAscii(v.getRawValue()));
		});
	}

	void printDatabases() throws Exception {
		Path file = logDir.resolve("getPeersDB.log");
		
		writeAndAtomicMove(file, writer -> dhts.stream().filter(DHT::isRunning).forEach(d -> {
			writer.append("Type: " + d.getType().shortName + "\n");
			formatDatabase(writer, d.getDatabase());
		}));
	}
	
	public void formatDatabase(Appendable writer, Database db) {
		Map<Key, PeersSeeds> items = db.getData();
		
		Formatter f = new Formatter(writer);
		
		f.format("Keys: %d Entries: %d%n", items.size(), items.values().stream().collect(Collectors.summingInt(l -> l.size())));
		
		items.entrySet().stream().sorted((a,b) -> b.getValue().size() - a.getValue().size()).forEach(e -> {
			PeersSeeds v = e.getValue();
			f.format("%s s:%5d p:%5d ∑:%5d%n", e.getKey().toString(false), v.seeds().size(), v.peers().size(), v.size());
		});
		
		f.format("%n======%n%n");
		
		
		Instant now = Instant.now();
		
		
		
		items.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEachOrdered((e) -> {
			Key k = e.getKey();
			PeersSeeds v = e.getValue();
			
			f.format("%s (s:%d p:%d)%n", k.toString(false), v.seeds().size(), v.peers().size());
			
			Stream.concat(v.seeds().stream(), v.peers().stream()).sorted((a,b) -> Arrays.compareUnsigned(a.getData(), b.getData())).forEachOrdered(i -> {
				unchecked(() -> writer.append("  ").append(i.toString()).append(" age: ").append(Duration.between(Instant.ofEpochMilli(i.getCreatedAt()), now).toString()).append('\n'));
			});
			
			
			f.format("%n");
		});
		
		f.format("%n%n");
	}
	
	private void printRoutingTable() throws Exception {
		Path file = logDir.resolve("routingTable.log");
		
		writeAndAtomicMove(file, writer -> dhts.stream().filter(DHT::isRunning).forEach(d -> this.formatRoutingTable(writer, d.getNode())));
	}
	
	public void formatRoutingTable(Appendable writer, Node node) {
		Collection<Key> localIds = node.localIDs();
		RoutingTable entries = node.table();
		Collection<NetMask> masks = node.getTrustedNetMasks();
		
		NavigableMap<Key, PeersSeeds> peerDB = new TreeMap<>(node.getDHT().getDatabase().getData());
		NavigableMap<Key, StorageItem> putDB = new TreeMap<>(node.getDHT().getStorage().getItems());
		
		Formatter f = new Formatter(writer);
		
		f.format("Type: %s%n", node.getDHT().getType().shortName);
		
		Consumer<Prefix> dbMapper = (Prefix p) -> {
			NavigableMap<Key, PeersSeeds> subPeers = peerDB.subMap(p.first(), true, p.last(), true);
			if(subPeers.isEmpty())
				f.format("%28s", "");
			else
				f.format("ihash:%5d s:%5d p:%5d ",
						subPeers.size(),
						subPeers.values().stream().mapToInt(e -> e.seeds().size()).sum(),
						subPeers.values().stream().mapToInt(e -> e.peers().size()).sum());
			
			NavigableMap<Key, StorageItem> subStorage = putDB.subMap(p.first(), true, p.last(), true);
			
			if(subStorage.isEmpty())
				f.format("%14s", "");
			else
				f.format("storage:%5d ", subStorage.size());
			
			
			return;
		};
		
		entries.stream().forEach(tableEntry -> {
			Optional<Key> localId = localIds.stream().filter(i -> tableEntry.prefix.isPrefixOf(i)).findAny();
			String isHomeBucket = localId.map(k -> "[Home:"+k.toString(false)+"]").orElse("");
			f.format("%s/%-3s main:%d rep:%d ", new Key(tableEntry.prefix).toString(false), tableEntry.prefix.getDepth(), tableEntry.getBucket().getNumEntries(), tableEntry.getBucket().getNumReplacements());
			dbMapper.accept(tableEntry.prefix);
			f.format("%s %s%n", tableEntry.prefix, isHomeBucket);
		});
		
		f.format("%n======%n%n");
		
		entries.stream().forEach(tableEntry -> {
			Optional<Key> localId = localIds.stream().filter(i -> tableEntry.prefix.isPrefixOf(i)).findAny();
			String isHomeBucket = localId.map(k -> "[Home:"+k.toString(false)+"]").orElse("");
			f.format("%40s/%-3d ", new Key(tableEntry.prefix).toString(false), tableEntry.prefix.getDepth());
			dbMapper.accept(tableEntry.prefix);
			f.format("%s%n", isHomeBucket);
			
			List<KBucketEntry> bucketEntries = tableEntry.getBucket().getEntries();
			if(bucketEntries.size() > 0) {
				f.format("  Entries (%d)%n", bucketEntries.size());
				bucketEntries.forEach(bucketEntry -> f.format("    %s %s%n", bucketEntry,masks.stream().anyMatch(m -> m.contains(bucketEntry.getAddress().getAddress())) ? "[trusted]" : ""));
			}
			List<KBucketEntry> replacements = tableEntry.getBucket().getReplacementEntries();
			if(replacements.size() > 0) {
				f.format("  Replacements (%d)%n", replacements.size());
				replacements.forEach(bucketEntry -> f.format("    %s%n", bucketEntry));
			}
			if(bucketEntries.size() > 0 || replacements.size() > 0)
				f.format("%n");
		});
		
		f.format("%n%n");
		
	}
	
	
	
	

	void printMain() throws Exception {
		Path diagnostics = logDir.resolve("diagnostics.log");
		
		writeAndAtomicMove(diagnostics, w -> dhts.stream().filter(DHT::isRunning).forEach(d -> d.printDiagnostics(w)));
	}
	
	
	static void writeAndAtomicMove(Path targetName, Consumer<PrintWriter> write) throws IOException {
		Path tempFile = Files.createTempFile(targetName.getParent(), targetName.getFileName().toString(), ".tmp");
		
		try (PrintWriter statusWriter = new PrintWriter(Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8))) {
			
			write.accept(statusWriter);

			statusWriter.close();
			Files.move(tempFile, targetName, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		}
	}
	
	
}
