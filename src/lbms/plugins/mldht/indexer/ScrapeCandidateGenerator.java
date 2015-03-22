package lbms.plugins.mldht.indexer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.hibernate.Session;
import org.hibernate.Transaction;

import lbms.plugins.mldht.indexer.assemblyline.AssemblyTask;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

public class ScrapeCandidateGenerator implements AssemblyTask {
	
	BlockingQueue<TorrentDBEntry> output;
	
	public ScrapeCandidateGenerator(BlockingQueue<TorrentDBEntry> outputPin) {
		this.output = outputPin;
	}
	
	public boolean performTask() {
		Set<TorrentDBEntry> toLookup = new TreeSet<TorrentDBEntry>();

		int maxLookups = output.remainingCapacity();
		
		if(maxLookups < 1)
			return false;
		
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();

		try
		{
			List<TorrentDBEntry> results = Collections.EMPTY_LIST;

			// get canidates for scrape
			// useindex(e, hitCountIdx) is true and 
			String query = "from ihdata e where e.status > 1 and e.hitCount > 0 and e.lastLookupTime < ? order by e.hitCount desc";

			results = session.createQuery(query)
				.setLong(0, System.currentTimeMillis()/1000 - 3600*12) // don't fetch more than once every 12 hours
				.setFirstResult(0)
				//.setComment("useIndex(e,primary)")
				.setMaxResults(maxLookups)
				.setFetchSize(maxLookups)
				.list();

			toLookup.addAll(results);

			// update in ascending order to avoid deadlocks
			long now = System.currentTimeMillis()/1000;
			for(TorrentDBEntry e : toLookup)
			{
				e.lastLookupTime = now;
				session.update(e);
			}

			tx.commit();
		} catch(Exception e)
		{
			tx.rollback();
			DHT.log(e, LogLevel.Info);
			return false;
		} finally {
			session.close();
		}
		
		output.addAll(toLookup);
		return true;
	}
}
