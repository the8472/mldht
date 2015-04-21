package the8472.mldht.cli;

import static the8472.bencode.Utils.buf2str;
import static the8472.utils.Functional.tap;
import static the8472.utils.Functional.unchecked;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import lbms.plugins.mldht.kad.DHT;
import the8472.bencode.BEncoder;
import the8472.mldht.cli.commands.Burst;
import the8472.mldht.cli.commands.GetTorrent;
import the8472.mldht.cli.commands.Help;
import the8472.mldht.cli.commands.Ping;

public abstract class CommandProcessor {
	
	protected Consumer<ByteBuffer> writer;
	protected Collection<DHT> dhts;
	protected List<byte[]> arguments;
	protected Path currentWorkDir = Paths.get("");
	
	BooleanSupplier active = () -> true;
	
	static Map<String, Class<? extends CommandProcessor>> SUPPORTED_COMMANDS = tap(new HashMap<>(), m -> {
		m.put("BURST", Burst.class);
		m.put("PING", Ping.class);
		m.put("HELP", Help.class);
		m.put("GETTORRENT", GetTorrent.class);
	});
	
	public static CommandProcessor from(List<byte[]> args, Consumer<ByteBuffer> writer, Collection<DHT> dhts) {
		String commandName = args.size() > 0 ? buf2str(ByteBuffer.wrap(args.get(0))).toUpperCase() : "HELP";
		Class<? extends CommandProcessor> clazz = Optional.<Class<? extends CommandProcessor>>ofNullable(SUPPORTED_COMMANDS.get(commandName)).orElse(Help.class);
		
		CommandProcessor proc = unchecked(() -> clazz.newInstance());
		proc.writer = writer;
		proc.dhts = dhts;
		proc.arguments = args.size() > 1 ? args.subList(1, args.size()) : Collections.emptyList();
		proc.process();
		return proc;
	}
	
	protected abstract void process();
	
	protected void handleException(Throwable ex) {
		Writer w = new CharArrayWriter(1024);
		PrintWriter pw = new PrintWriter(w);
		ex.printStackTrace(pw);
		printErr(w.toString());
		exit(200);
	}
	
	protected void println(String str) {
		Map<String, Object> map = new HashMap<>();
		map.put("action", "sysout");
		byte[] bytes = (str + '\n').getBytes(StandardCharsets.UTF_8);
		map.put("payload", bytes);
		writer.accept(new BEncoder().encode(map, bytes.length + 40));
	}

	protected void printErr(String str) {
		Map<String, Object> map = new HashMap<>();
		map.put("action", "syserr");
		map.put("payload", str.getBytes(StandardCharsets.UTF_8));
		writer.accept(new BEncoder().encode(map, str.length()*4 + 40));
	}
	
	protected void exit(int code) {
		active = () -> false;
		Map<String, Object> map = new HashMap<>();
		map.put("action", "exit");
		map.put("exitCode", code);
		writer.accept(new BEncoder().encode(map, 64));
	}
	
	protected boolean isRunning() {
		return active.getAsBoolean();
	}
	

}
