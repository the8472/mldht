package lbms.plugins.mldht.indexer.db;


import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lbms.plugins.mldht.indexer.HibernateUtil;

import org.hibernate.EmptyInterceptor;

public class IndexHintInterceptor extends EmptyInterceptor
{
    private static final long serialVersionUID = 1L;
 
    @Override
    public String onPrepareStatement(String sql)
    {
    	
    	// select torrentdbe0_.id as id0_, torrentdbe0_.added as added0_, torrentdbe0_.fetchAttemptCount as fetchAtt3_0_, torrentdbe0_.hitCount as hitCount0_, torrentdbe0_.info_hash as info5_0_, torrentdbe0_.lastFetchAttempt as lastFetc6_0_, torrentdbe0_.lastSeen as lastSeen0_, torrentdbe0_.status as status0_ from ihdata torrentdbe0_ where useindex(torrentdbe0_.id, infohashIdx)=true and torrentdbe0_.info_hash>? and torrentdbe0_.info_hash<? and torrentdbe0_.status=0 and torrentdbe0_.hitCount>0 and torrentdbe0_.lastFetchAttempt<? order by torrentdbe0_.info_hash limit ?
    	ArrayList<String> tables = new ArrayList<String>();
    	ArrayList<String> indices = new ArrayList<String>();
    	StringBuffer processedQuery = new StringBuffer();
    	
    	Matcher m = Pattern.compile("useindex\\((.+)\\.(?:.+),(.+)\\)=(1|true)( and|)").matcher(sql);
    	
    	while(m.find()) {
    		tables.add(m.group(1).trim());
    		indices.add(m.group(2).trim());
    		
    		m.appendReplacement(processedQuery, "");
    	}
    	
    	m.appendTail(processedQuery);
    	
    	
    	if(HibernateUtil.isMySQL())
    	{
    		for(int i = 0;i<tables.size();i++)
    		{
    			String table = tables.get(i);
    			String index = indices.get(i);
    			
    			int insertPoint = processedQuery.indexOf(table) + table.length();
    			processedQuery.insert(insertPoint, " use index("+index+") ");
    			
    		}
    			
    	
    	}
    	
    	return processedQuery.toString();
    }
 
    protected void throwError(String message)
    {
        throw new IllegalStateException(message);
    }

}