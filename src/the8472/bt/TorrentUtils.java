package the8472.bt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import the8472.bencode.BDecoder;
import the8472.bencode.BEncoder;

public class TorrentUtils {
	
	public static ByteBuffer wrapBareInfoDictionary(ByteBuffer dict) {
		Map<String, Object> root = new HashMap<>();
		
		root.put("info", new BEncoder.RawData(dict));
		
		BEncoder encoder = new BEncoder();
		return encoder.encode(root, dict.remaining() + 100);
	}
	
	public static Optional<String> getTorrentName(ByteBuffer torrent) {
		BDecoder decoder = new BDecoder();
		Map<String, Object> root = decoder.decode(torrent.duplicate());
		
		return Optional.ofNullable((Map<String, Object>)root.get("info")).map(info -> {
			return Optional.ofNullable((byte[])info.get("name.utf-8")).orElse((byte[])info.get("name"));
		}).map(bytes -> new String(bytes, StandardCharsets.UTF_8));
	}
	

}
