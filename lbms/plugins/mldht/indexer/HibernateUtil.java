package lbms.plugins.mldht.indexer;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;

public class HibernateUtil {
	
	private static SessionFactory	sessionFactory;
	
	static
	{
		try
		{
			//By default it will look for hibernate.cfg.xml in the class path
			sessionFactory = new AnnotationConfiguration().configure().buildSessionFactory();
		} catch (Throwable ex)
		{
			throw new ExceptionInInitializerError(ex);
		}
	}

	public static SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public static void shutdown() {
		//Close caches and connection pool
		getSessionFactory().close();
	}
}
