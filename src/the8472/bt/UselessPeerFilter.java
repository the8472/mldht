package the8472.bt;

import static the8472.utils.Functional.typedGet;
import static the8472.utils.Functional.uncheckedCons;

import the8472.bencode.BEncoder;
import the8472.bt.PullMetaDataConnection.CloseReason;

import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class UselessPeerFilter {
	
	Path path;
	
	public UselessPeerFilter(Path p) throws IOException {
		path = p;
		Files.createDirectories(p);
	}
	
	public void insert(PullMetaDataConnection toAdd) {
		if(toAdd.closeReason == null) {
			throw new IllegalArgumentException("peer connection not closed yet");
		}
		
		if(toAdd.closeReason == CloseReason.OTHER)
			return;
		
		
		
		TreeMap<String, Object> dict = new TreeMap<>();
		dict.put("reason", toAdd.closeReason.name());
		dict.put("created", System.currentTimeMillis());
		
		try(FileChannel chan = FileChannel.open(path.resolve(nameFromAddr(toAdd.destination)), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
			chan.write(new BEncoder().encode(dict, 1024));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	public boolean isBad(InetSocketAddress addr) {
		return Files.exists(path.resolve(nameFromAddr(addr)));
	}
	
	String nameFromAddr(InetSocketAddress addr) {
		// : is reserved on windows
		return addr.getAddress().getHostAddress().replace(':', '_') + ";" + addr.getPort();
	}
	
	public void clean() throws IOException {
		
		long now = System.currentTimeMillis();
		
		try(Stream<Path> st = Files.list(path)) {
			st.forEach(uncheckedCons(p -> {
				Map<String, Object> map = ThreadLocalUtils.getDecoder().decode(ByteBuffer.wrap(Files.readAllBytes(p)));
				CloseReason r = CloseReason.valueOf(typedGet(map, "reason", byte[].class).map(b -> new String(b, StandardCharsets.ISO_8859_1)).orElse(null));
				long created = typedGet(map, "created", Long.class).orElse(0L);
				
				long timeout = 0;
				
				switch(r) {
					case CONNECT_FAILED:
						timeout = TimeUnit.MINUTES.toMillis(10);
						break;
					default:
						timeout = TimeUnit.HOURS.toMillis(2);
				}
				
				if(now - created > timeout)
					Files.deleteIfExists(p);
			}));
		}
	}
	
	

}
