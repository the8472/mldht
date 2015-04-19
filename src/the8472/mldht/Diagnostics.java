package the8472.mldht;

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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lbms.plugins.mldht.kad.DBItem;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Database;
import lbms.plugins.mldht.kad.KBucketEntry;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Node;
import lbms.plugins.mldht.kad.Node.RoutingTableEntry;
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
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void printDatabases() throws Exception {
		Path file = logDir.resolve("getPeersDB.log");
		
		writeAndAtomicMove(file, writer -> dhts.stream().filter(DHT::isRunning).forEach(d -> {
			writer.append("Type: " + d.getType().shortName + "\n");
			formatDatabase(writer, d.getDatabase());
		}));
	}
	
	public void formatDatabase(Appendable writer, Database db) {
		Map<Key, List<DBItem>> items = db.getData();
		
		Formatter f = new Formatter(writer);
		
		f.format("Keys: %d Entries: %d%n", items.size(), items.values().stream().collect(Collectors.summingInt(l -> l.size())));
		
		items.entrySet().stream().sorted((a,b) -> b.getValue().size() - a.getValue().size()).forEach(e -> {
			f.format("%s: %4d%n", e.getKey().toString(false), e.getValue().size());
		});
		
		f.format("%n======%n%n");
		
		
		Instant now = Instant.now();
		
		items.entrySet().stream().sorted((a,b) -> a.getKey().compareTo(b.getKey())).forEach((e) -> {
			Key k = e.getKey();
			List<DBItem> v = e.getValue();
			
			f.format("%s (%d)%n", k.toString(false), v.size());
			
			v.stream().sorted((a,b) -> Arrays.compareUnsigned(a.getData(), b.getData())).forEachOrdered(i -> {
				f.format("  %s age: %s%n", i.toString(), Duration.between(Instant.ofEpochMilli(i.getCreatedAt()), now));
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
		List<RoutingTableEntry> entries = node.getBuckets();
		Collection<NetMask> masks = node.getTrustedNetMasks();
		
		Formatter f = new Formatter(writer);
		
		f.format("Type: %s%n", node.getDHT().getType().shortName);
		
		entries.forEach(tableEntry -> {
			Optional<Key> localId = localIds.stream().filter(i -> tableEntry.prefix.isPrefixOf(i)).findAny();
			String isHomeBucket = localId.map(k -> "[Home:"+k.toString(false)+"]").orElse("");
			f.format("%s/%-3s main:%d rep:%d %s %s%n", new Key(tableEntry.prefix).toString(false), tableEntry.prefix.getDepth(), tableEntry.getBucket().getNumEntries(), tableEntry.getBucket().getNumReplacements(), tableEntry.prefix, isHomeBucket);
		});
		
		f.format("%n======%n%n");
		
		entries.forEach(tableEntry -> {
			Optional<Key> localId = localIds.stream().filter(i -> tableEntry.prefix.isPrefixOf(i)).findAny();
			String isHomeBucket = localId.map(k -> "[Home:"+k.toString(false)+"]").orElse("");
			f.format("%40s/%-3d %s%n", new Key(tableEntry.prefix).toString(false), tableEntry.prefix.getDepth(), isHomeBucket);
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
