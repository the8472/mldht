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

import lbms.plugins.mldht.indexer.db.IndexHintInterceptor;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.cfg.Configuration;

public class HibernateUtil {
	
	private static SessionFactory	sessionFactory;
	
	private static boolean isMySQL = false; 
	
	static
	{
		try
		{
			//By default it will look for hibernate.cfg.xml in the class path
			Configuration config = new Configuration().configure();
			config.addAnnotatedClass(TorrentDBEntry.class);
			config.addAnnotatedClass(ScrapeDBEntry.class);
			config.setInterceptor(new IndexHintInterceptor());
			if(config.getProperty("hibernate.dialect").contains("MySQL"))
				isMySQL = true;
				
			sessionFactory = config.buildSessionFactory();
		} catch (Throwable ex)
		{
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	public static boolean isMySQL() {
		return isMySQL;
	}

	public static SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public static void shutdown() {
		//Close caches and connection pool
		getSessionFactory().close();
	}
}
