package the8472.mldht.cli;

import static the8472.utils.Functional.typedGet;

import the8472.bencode.PathMatcher;
import the8472.bencode.PrettyPrinter;
import the8472.bencode.Tokenizer;
import the8472.bencode.Tokenizer.BDecodingException;
import the8472.utils.concurrent.SerializedTaskExecutor;

import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TorrentInfo {
	
	Path source;
	ByteBuffer raw;
	Map<String, Object> root;
	Map<String, Object> info;
	Charset encoding = StandardCharsets.UTF_8;
	
	
	public TorrentInfo(Path source) {
		this.source = source;
	}
	
	void readRaw() {
		if(raw != null)
			return;
		try(FileChannel chan = FileChannel.open(source, StandardOpenOption.READ)) {
			raw = chan.map(MapMode.READ_ONLY, 0, chan.size());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	void decode() {
		if(root != null)
			return;
		readRaw();
		root = ThreadLocalUtils.getDecoder().decode(raw.duplicate());
		typedGet(root, "info", Map.class).ifPresent(i -> info = i);
		if(info != null) {
			String charset = typedGet(info, "encoding", byte[].class).map(b -> new String(b, StandardCharsets.ISO_8859_1)).orElse(null);
			if(charset != null) {
				try {
					this.encoding = Charset.forName(charset);
				} catch (Exception e) {
					System.err.println("Charset " + charset + "not supported, falling back to " + encoding.name());
				}
			}
		}
	}
	
	Key infoHash() {
		
		Tokenizer t = new Tokenizer();
		PathMatcher m = new PathMatcher("info");
		m.tokenizer(t);
		
		ByteBuffer rawInfo = m.match(raw.duplicate());
		
		MessageDigest dig = ThreadLocalUtils.getThreadLocalSHA1();
		dig.reset();
		dig.update(rawInfo);
		return new Key(dig.digest());
	}
	
	String name() {
		decode();
		String name = typedGet(info, "name.utf-8", byte[].class).map(b -> new String(b, StandardCharsets.UTF_8)).orElse(null);
		if(name == null) {
			name = typedGet(info, "name", byte[].class).map(b -> new String(b, encoding)).orElseThrow(() -> new IllegalArgumentException("no name found"));
		}
		
		return name;
	}
	
	Stream<String> files() {
		Optional<List<?>> files = typedGet(info, "files", List.class);
		/*
					List<?> files = l;
					files.stream().filter(Map.class::isInstance).map(f -> (Map<String, Object>)f).forEachOrdered(file -> {
						typedGet(file, "path", List.class).map(p -> (List<?>)p).ifPresent(path -> {
							System.out.println(path.stream().filter(byte[].class::isInstance).map(b -> (byte[])b).map(b -> new String(b, e2)).collect(Collectors.joining(File.pathSeparator)));
						});
					});
		 */
		return Stream.empty();
	}
	
	String raw() {
		decode();
		PrettyPrinter p = new PrettyPrinter();
		p.indent("  ");
		p.guessHumanReadableStringValues(true);
		p.append(root);
		return p.toString();
	}


	public static void main(String[] argsAry) throws IOException, InterruptedException {
		List<String> args = new ArrayList<>(Arrays.asList(argsAry));
		
		boolean printRaw = ParseArgs.extractBool(args, "-raw");
		
		
		Stream<Path> files = args.parallelStream().unordered().map(Paths::get).filter(Files::exists).flatMap(p -> {
			try {
				return Files.find(p, 1, (f, attr) -> {
					return attr.isRegularFile() && attr.size() > 0;
				},  FileVisitOption.FOLLOW_LINKS);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}).collect(Collectors.toSet()).parallelStream().unordered();

		
		Consumer<String> printer = SerializedTaskExecutor.runSerialized((String s) -> {
			System.out.println(s);
		});
		
		files.map(p -> {
			TorrentInfo ti = new TorrentInfo(p);
			try {
				ti.decode();
			} catch(BDecodingException ex) {
				return p.toString() + " does not appear to be a bencoded file: " + ex.getMessage();
			}
			
			
			String st = p.toString();
			
			if(printRaw)
				return st + "\n" + ti.raw() + '\n';
			
			
			return st + " " + ti.name();
		}).forEach(printer::accept);
		

	}

}
