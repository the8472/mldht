package lbms.plugins.mldht.indexer;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import lbms.plugins.mldht.indexer.assemblyline.AssemblyTask;
import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.DHT.LogLevel;

public class OrderedBatchQueryRunner implements AssemblyTask {
	
	public static final int MAX_QUERY_CHARGE = 100;
	
	BlockingQueue<BatchQuery> queries;
	
	
	public OrderedBatchQueryRunner(BlockingQueue<BatchQuery> queries) {
		this.queries = queries;
		
	}
	
	
	public boolean performTask() {
		TreeSet<BatchQuery> toProcess = new TreeSet<BatchQuery>();
		BatchQuery toDrain = null;
		while((toDrain = queries.poll()) != null)
		{
			if(!toProcess.add(toDrain))
			{
				queries.add(toDrain);
				break;
			}
			
			if(toProcess.size() == MAX_QUERY_CHARGE)
				break;
		}
		
		if(toProcess.isEmpty())
			return false;
		
		Session s = null;
		Transaction tx = null;
		try {
			s = HibernateUtil.getSessionFactory().openSession();
			tx = s.beginTransaction();
			for(BatchQuery toRun : toProcess)
			{
				toRun.session = s;
				toRun.run();
			}
			tx.commit();
		} catch (HibernateException e)
		{
			tx.rollback();
			// something deadlocked? put everything back into the todo list that hasn't been processed yet
			queries.addAll(toProcess);
			DHT.log(e, LogLevel.Error);
			return false;
		} finally {
			s.close();
		}
		return true;
	}
}
