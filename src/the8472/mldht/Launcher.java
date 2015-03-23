package the8472.mldht;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHT.DHTtype;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.DHTLogger;
import the8472.utils.ConfigReader;
import the8472.utils.FilesystemNotifications;
import the8472.utils.XMLUtils;
import the8472.utils.io.NetMask;

public class Launcher {
	
	private static final String BOOTSTRAP_KEY = "noRouterBootstrap";
	private static final String PERSIST_ID = "persistId";
	private static final String CACHE_PATH = "routingTableCachePath";
	private static final String PORT = "port";
	private static final String MULTIHOMING = "multihoming";
	private static final String LOG_LEVEL = "logLevel";
	
	Map<String, Object> defaults = new HashMap<>();
	
	{
		defaults.put(BOOTSTRAP_KEY , false);
		defaults.put(PERSIST_ID, true);
		defaults.put(CACHE_PATH, "./dht.cache");
		defaults.put(PORT, 49001);
		defaults.put(MULTIHOMING, true);
		defaults.put(LOG_LEVEL, LogLevel.Verbose.name());
	}
	
	Supplier<InputStream> configSchema = () -> Launcher.class.getResourceAsStream("config.xsd");
	
	Supplier<InputStream> configDefaults = () -> Launcher.class.getResourceAsStream("config-defaults.xml");
	
	List<Component> components = new ArrayList<>();
	
	private ConfigReader configReader = new ConfigReader(Paths.get(".", "config.xml"), configDefaults, configSchema);
	
	{
		configReader.read();
	}

	DHTConfiguration config = new DHTConfiguration() {
		

		
		@Override
		public boolean noRouterBootstrap() {
			return !configReader.getBoolean("//core/useBootstrapServers").orElse(true);
		}

		@Override
		public boolean isPersistingID() {
			return configReader.getBoolean("//core/persistID").orElse(true);
		}

		@Override
		public File getNodeCachePath() {
			return new File("./dht.cache");
		}

		@Override
		public int getListeningPort() {
			return configReader.getLong("//core/port").orElse(49001L).intValue();
		}

		@Override
		public boolean allowMultiHoming() {
			return configReader.getBoolean("//core/multihoming").orElse(true);
		}
	};

	protected Map<DHTtype, DHT> dhts = DHT.createDHTs();

	volatile boolean running = true;

	Thread shutdownHook = new Thread(this::onVmShutdown, "shutdownHook");

	private void onVmShutdown() {
		running = false;
		stop();
	}
	
	FilesystemNotifications notifications = new FilesystemNotifications();


	protected void start() throws Exception {
		
		Path logDir = Paths.get("./logs/");
		Files.createDirectories(logDir);
		

		Path log = logDir.resolve("dht.log");
		Path exLog = logDir.resolve("execptions.log");

		final PrintWriter logWriter = new PrintWriter(Files.newBufferedWriter(log, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE), true);
		final PrintWriter exWriter = new PrintWriter(Files.newBufferedWriter(exLog, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE), true);
		
		configReader.getAll(XMLUtils.buildXPath("//component/className",null)).forEach(className -> {
			try {
				Class<Component> clazz = (Class<Component>) Class.forName(className);
				components.add(clazz.newInstance());
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
		});
 
		DHT.setLogger(new DHTLogger() {

			private String timeFormat(LogLevel level) {
				return "[" + Instant.now().toString() + "][" + level.toString() + "] ";
			}

			TransferQueue<String> toLog = new LinkedTransferQueue<>();

			Thread writer = new Thread() {
				@Override
				public void run() {
					while (true) {
						try {
							logWriter.println(toLog.take());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			};

			{
				writer.setDaemon(true);
				writer.setName("LogWriter");
				writer.start();
			}

			public void log(String message, LogLevel l) {
				try {
					toLog.put(timeFormat(l) + message);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			public void log(Throwable e, LogLevel l) {
				exWriter.append(timeFormat(l));
				e.printStackTrace(exWriter);
			}
		});

		Path diagnostics = logDir.resolve("diagnostics.log");

		DHT.getScheduler().scheduleWithFixedDelay(
				() -> {

					try {
						Path tempFile = Files.createTempFile(diagnostics.getParent(), "diag", ".tmp");

						try (PrintWriter statusWriter = new PrintWriter(Files.newBufferedWriter(tempFile,
								StandardCharsets.UTF_8))) {
							for (DHT dht : dhts.values()) {
								dht.printDiagnostics(statusWriter);
							}

							statusWriter.close();

							Files.move(tempFile, diagnostics, StandardCopyOption.ATOMIC_MOVE,
									StandardCopyOption.REPLACE_EXISTING);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}, 10, 30, TimeUnit.SECONDS);

		setLogLevel();
		configReader.registerFsNotifications(notifications);
		configReader.addChangeCallback(this::setLogLevel);

		for (DHT dht : dhts.values()) {
			if(isIPVersionDisabled(dht.getType().PREFERRED_ADDRESS_TYPE))
				continue;
			dht.start(config);
			dht.bootstrap();
			// dht.addIndexingListener(dumper);
		}
		
		// need to run this after startup, Node doesn't exist before then
		setTrustedMasks();
		configReader.addChangeCallback(this::setTrustedMasks);
		
		components.forEach(c -> c.start(dhts.values(), configReader));

		Runtime.getRuntime().addShutdownHook(shutdownHook);
		
		Path shutdown = Paths.get("./shutdown");
		
		if(!Files.exists(shutdown))
			Files.createFile(shutdown);
		
		notifications.addRegistration(shutdown, (path, kind) -> {
			if(path.equals(shutdown)) {
				stop();
			}
		});
		
		// need 1 non-daemon-thread to keep VM alive
		while(running) {
			synchronized (this) {
				this.wait();
			}
		}
	}
	
	private void setLogLevel() {
		String rawLevel = configReader.get(XMLUtils.buildXPath("//core/logLevel")).orElse("Info");
		LogLevel level = LogLevel.valueOf(rawLevel);
		DHT.setLogLevel(level);
	}
	
	private void setTrustedMasks() {
		Collection<NetMask> masks = configReader.getAll(XMLUtils.buildXPath("//core/clusterNodes/networkPrefix")).map(NetMask::fromString).collect(Collectors.toList());
		dhts.values().forEach((d) -> {
			if(d.isRunning())
				d.getNode().setTrustedNetMasks(masks);
		});
	}
	
	private boolean isIPVersionDisabled(Class<? extends InetAddress> type) {
		long disabled = configReader.getLong("//core/disableIPVersion").orElse(-1L);
		if(disabled == 6 && type.isAssignableFrom(Inet6Address.class))
			return true;
		if(disabled == 4 && type.isAssignableFrom(Inet4Address.class))
			return true;
		return false;
	}

	public void stop() {
		if(running) {
			synchronized (this) {
				this.notifyAll();
			}
			running = false;
			components.forEach(Component::stop);
			dhts.values().forEach(DHT::stop);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		new Launcher().start();
	}

}
