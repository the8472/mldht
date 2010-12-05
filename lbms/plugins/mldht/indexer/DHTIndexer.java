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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import lbms.plugins.mldht.kad.DHTLogger;

public class DHTIndexer {
	
	public static final ScheduledThreadPoolExecutor indexerScheduler;
	
	static {
		final ThreadGroup executorGroup = new ThreadGroup("DHT Indexer");
		int threads = 8;
		indexerScheduler = new ScheduledThreadPoolExecutor(threads, new ThreadFactory() {
			public Thread newThread (Runnable r) {
				Thread t = new Thread(executorGroup, r, "DHT Indexer Executor");

				t.setDaemon(true);
				return t;
			}
		});
		indexerScheduler.setCorePoolSize(threads);
		indexerScheduler.setMaximumPoolSize(threads*2);
		indexerScheduler.setKeepAliveTime(20, TimeUnit.SECONDS);
		indexerScheduler.allowCoreThreadTimeOut(true);
	}
	
	
}
