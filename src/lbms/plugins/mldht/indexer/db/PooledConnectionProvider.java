package lbms.plugins.mldht.indexer.db;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lbms.plugins.mldht.indexer.DHTIndexer;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.DHTLogger;
import lbms.plugins.mldht.kad.DHT.LogLevel;

import org.hibernate.HibernateException;
import org.hibernate.connection.ConnectionProvider;

public class PooledConnectionProvider implements ConnectionProvider {
	
	Properties connectConfig;
	Driver driver;
	String jdbcUrl;
	ConcurrentLinkedQueue<Connection> connectionPool = new ConcurrentLinkedQueue<Connection>();
	ScheduledFuture<?> poolCleaner;

	public void close() throws HibernateException {
		poolCleaner.cancel(false);
		Connection c = null;
		try
		{
			while((c = connectionPool.poll()) != null)
				c.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	public void closeConnection(Connection toClose) throws SQLException {
		if(!toClose.isClosed())
			connectionPool.add(toClose);
	}
	
	private void cleanPool() {
		try
		{
			Connection c = null;
			while((c = connectionPool.poll()) != null)
			{
				if(!c.isClosed() && connectionPool.size() < DHTIndexer.indexerScheduler.getPoolSize() && c.isValid(2))
				{
					connectionPool.add(c);
					break;
				}
					
				c.close();
			}
		} catch (SQLException e)
		{
			DHT.log(e, LogLevel.Error);
		}
	}

	public void configure(Properties hibernateConfig) throws HibernateException {
		try
		{
			Class.forName(hibernateConfig.getProperty("hibernate.connection.driver_class"));
			jdbcUrl = (String) hibernateConfig.get("hibernate.connection.url");
			driver = DriverManager.getDriver(jdbcUrl);
			connectConfig = new Properties();
			connectConfig.put("user", hibernateConfig.get("hibernate.connection.username"));
			connectConfig.put("password", hibernateConfig.get("hibernate.connection.password"));
			poolCleaner = DHTIndexer.indexerScheduler.scheduleWithFixedDelay(new Runnable() {
				public void run() {
					cleanPool();
				}
			}, 10, 10, TimeUnit.SECONDS);
		} catch (Exception e)
		{
			throw new HibernateException("could not initialize connection provider", e);			
		}
	}

	public Connection getConnection() throws SQLException {
		Connection c = connectionPool.poll();
		if(c == null || c.isClosed())
		{
			c = driver.connect(jdbcUrl, connectConfig);
			c.setAutoCommit(false);
			c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		}
		return c;
	}

	public boolean supportsAggressiveRelease() {
		return false;
	}
	
	
}
