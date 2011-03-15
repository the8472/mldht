/*
 *    This file is part of mlDHT. 
 * 
 *    mlDHT is free software: you can redistribute it and/or modify 
 *    it under the terms of the GNU General Public License as published by 
 *    the Free Software Foundation, either version 2 of the License, or 
 *    (at your option) any later version. 
 * 
 *    mlDHT is distributed in the hope that it will be useful, 
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *    GNU General Public License for more details. 
 * 
 *    You should have received a copy of the GNU General Public License 
 *    along with mlDHT.  If not, see <http://www.gnu.org/licenses/>. 
 */
package lbms.plugins.mldht.indexer;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

import lbms.plugins.mldht.DHTConfiguration;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTIndexingListener;
import lbms.plugins.mldht.kad.DHTLogger;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.DHTtype;

public class Main {
	
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
			return false;
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
		
		InfoHashGatherer dumper = new InfoHashGatherer();
		
		for (DHT dht : dhts.values()) {
			dht.start(config);
			dht.bootstrap();
			dht.addIndexingLinstener(dumper);
		}

		
		MetaDataGatherer finder = new MetaDataGatherer(dumper);
		dumper.setMetaDataGatherer(finder);

		BufferedReader con = new BufferedReader(new InputStreamReader(System.in));
		
		DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try
				{
					PrintWriter statusWriter = null;
					try {
						statusWriter = new PrintWriter("./diagnostics.log");
						for (DHT dht : dhts.values()) {
							statusWriter.print(dht.getDiagnostics()); 
						}
					} finally {
						statusWriter.close();						
					}
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}, 10, 30, TimeUnit.SECONDS);
		
		
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
		new Main().start();
	}
}
