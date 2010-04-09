package lbms.plugins.mldht.indexer;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.Transaction;

import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTIndexingListener;
import lbms.plugins.mldht.kad.DHTLogger;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.DHTtype;

public class IndexingService {
	
	DHTConfiguration config = new DHTConfiguration() {
		@Override
		public boolean noRouterBootstrap() {
			return false;
		}
		
		@Override
		public boolean isPersistingID() {
			return true;
		}
		
		@Override
		public File getNodeCachePath() {
			return new File("./dht.cache");
		}
		
		@Override
		public int getListeningPort() {
			return 49001;
		}
		
		@Override
		public boolean allowMultiHoming() {
			return true;
		}
	};
	
	Map<DHTtype, DHT>	dhts	= DHT.createDHTs();
	
	private void start() throws Exception {
		
		// do some trickery to prevent too much garbage from being loaded in
		System.setProperty("transitory.startup","1");
		
		try 
		{
			Class<?> clazz = Class.forName("org.gudy.azureus2.core3.util.Timer");
			Field field = clazz.getDeclaredField("DEBUG_TIMERS");
			field.setAccessible(true);
			field.setBoolean(null, false);
		} catch (Throwable t)
		{
			t.printStackTrace();
		}
		
		final PrintWriter logWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./dht.log"))),true);
		final PrintWriter exWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./exceptions.log"))),true); 
		
		DHT.setLogger(new DHTLogger() {

			public void log (String message) {
				logWriter.println(message);
			}

			public void log (Exception e) {
				e.printStackTrace(exWriter);

			}
		});
		
		//final PrintWriter hashWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("./infohashQueries.log",true))),true);
		
		DHTIndexingListener listener = new DHTIndexingListener() {
			@Override
			public void incomingPeersRequest(Key infoHash, InetAddress sourceAddress, Key nodeID) {
				//hashWriter.println(infoHash.toString(false)+"\tfrom: "+sourceAddress.getHostAddress()+"/"+nodeID.toString(false));
				
				Session session = HibernateUtil.getSessionFactory().openSession();
				Transaction tx = session.beginTransaction();
				String hash = infoHash.toString(false);
				TorrentDBEntry entry = (TorrentDBEntry) session.get(TorrentDBEntry.class, hash);
				if(entry == null)
				{
					entry = new TorrentDBEntry();
					entry.info_hash = hash;
					entry.added = System.currentTimeMillis();
					entry.status = 0;
					session.save(entry);
				}
				
				tx.commit();
				session.close();
				
				
			}
		};
		
		for (Map.Entry<DHTtype, DHT> e : dhts.entrySet()) {
			DHT dht = e.getValue();
			dht.start(config);
			dht.bootstrap();
			dht.addIndexingLinstener(listener);
		}
		

		BufferedReader con = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Type 'quit' to shutdown");
		
		while(true)
		{
			String line = con.readLine();
			if(line.equals("quit"))
				break;
		}

		for (Map.Entry<DHTtype, DHT> e : dhts.entrySet()) {
			DHT dht = e.getValue();
			dht.stop();
		}
		
		HibernateUtil.shutdown();
		
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		new IndexingService().start();
	}
}
