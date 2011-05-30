package lbms.plugins.mldht.indexer;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.hibernate.Session;
import org.hibernate.Transaction;

import lbms.plugins.mldht.indexer.assemblyline.AssemblyTask;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

public class FetchCandidateGenerator implements AssemblyTask {

	public static final int PIVOT_EVERY_N_VIRTUAL_NODES = 3;
	public static final int MIN_PIVOTS = 32;
	public static final int MAX_LOOKUPS_PER_PIVOT = 9;
	
	MetaDataGatherer container;
	LinkedBlockingQueue<TorrentDBEntry> output;
	ArrayList<Key> lookupPivotPoints = new ArrayList<Key>();
	
	
	public FetchCandidateGenerator(MetaDataGatherer container) {
		this.container = container;
		this.output = container.fetchDHTlink;
	}
	
	public boolean performTask() {
		if(new File("./torrents/").getUsableSpace() < 512*1024*1024)
			return false;
		
		Set<TorrentDBEntry> toLookup = new TreeSet<TorrentDBEntry>();
		//TorrentDBEntry result = null;
		
		
		int maxLookups = Math.min(MAX_LOOKUPS_PER_PIVOT, output.remainingCapacity());
		
		if(maxLookups < 1)
			return false;
		
		updatePivots();
		
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		ArrayList<Key> pivotBackup = (ArrayList<Key>) lookupPivotPoints.clone();

		try
		{

			int pivotIdx = ThreadLocalUtils.getThreadLocalRandom().nextInt(lookupPivotPoints.size());
			Key startKey = lookupPivotPoints.get(pivotIdx);
			Key endKey = pivotIdx+1 == lookupPivotPoints.size() ? Key.MAX_KEY : lookupPivotPoints.get(pivotIdx+1);

			
			List<TorrentDBEntry> results = Collections.EMPTY_LIST;

			// get canidates for combined scrape+retrieve
			String query = "from ihdata e where useindex(e, infohashIdx) is true and (e.info_hash > ? and e.info_hash < ?) and e.status = ? and e.hitCount > 0 and e.lastLookupTime < ? order by e.info_hash";

			results = session.createQuery(query)
			.setBinary(0, startKey.getHash())
			.setBinary(1, endKey.getHash())
			.setInteger(2, TorrentDBEntry.STATE_METADATA_NEVER_OBTAINED)
			.setLong(3, System.currentTimeMillis()/1000 - 3600) // don't fetch more than once an hour
			.setFirstResult(0)
			//.setComment("useIndex(e,primary)")
			.setMaxResults(maxLookups)
			.setFetchSize(maxLookups)
			.list();


			lookupPivotPoints.remove(startKey);
			if(results.size() > 0)
			{
				System.out.println(startKey+" -> "+new Key(results.get(results.size()-1).info_hash));
				lookupPivotPoints.add(new Key(results.get(results.size()-1).info_hash));
			} else if(endKey == Key.MAX_KEY)
			{
				System.out.println(startKey+" -> "+Key.MIN_KEY);
				lookupPivotPoints.add(Key.MIN_KEY);
			} else
				System.out.println(startKey+" -> -");
					

					
			Collections.sort(lookupPivotPoints);				

			toLookup.addAll(results);

			// update in ascending order to avoid deadlocks
			long now = System.currentTimeMillis()/1000;
			for(TorrentDBEntry e : toLookup)
			{
				e.lastLookupTime = now;
				if(e.status == 0)
					e.status = 1;
				session.update(e);
			}
				

			tx.commit();
		} catch(Exception e)
		{
			tx.rollback();
			DHT.log(e, LogLevel.Info);
			// restore on failure
			lookupPivotPoints = pivotBackup;
			// rolled back, don't start lookups
			return false;
		} finally {
			session.close();
		}
		
		output.addAll(toLookup);
		return !toLookup.isEmpty();
	}


	private void updatePivots() {
		int numPivots = Math.max(MIN_PIVOTS, container.getNumVirtualNodes() / PIVOT_EVERY_N_VIRTUAL_NODES);
		Key k = Key.createRandomKey();
		int i = 0;
		while(lookupPivotPoints.size() < numPivots)
			lookupPivotPoints.add(k.getDerivedKey(i++));
		while(lookupPivotPoints.size() > numPivots)
			lookupPivotPoints.remove(lookupPivotPoints.size()-1);
		
		Collections.sort(lookupPivotPoints);
	}

	
}
