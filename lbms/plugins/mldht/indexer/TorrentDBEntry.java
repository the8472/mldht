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

import javax.persistence.*;

import org.hibernate.annotations.Index;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.OptimisticLockType;


@Entity(name="ihdata")
@org.hibernate.annotations.Entity(optimisticLock=OptimisticLockType.DIRTY,dynamicUpdate=true)
public class TorrentDBEntry implements Comparable<TorrentDBEntry> {
	
	@GeneratedValue(strategy=GenerationType.AUTO)
	@Id
	@Column(insertable=false)
	int id;
	
	@NaturalId
	@Column(length=20,unique=true)
	@Index(name="infohashIdx")
	byte[] info_hash;

	
	/**
	*  0 = do not have metadata
	*  1 = client currently trying to fetch metadata
	*  2 = we have fetched the metadata and it has to be uploaded to the indexer
	*  3 = torrent is private
	*  4 = metadata uploaded to indexer/already on indexer
	*  5 = metadata retrieved/available but not on local disk (error state)
	*  6 = indexer giving up (more than 10 failed upload attempts) (error state)
	*/	
	@Index(name="statusIdx")
	int status;
	
	/**
	 * hits, as seen by the DHT
	 */
	int hitCount;
	
	/**
	 *   unix_timestamp() of torrent added to the db
	*/
	@Column(length=10)
	long added;
	
	/**
	 * unix_timestamp()  of event resulting in status update to 0, 1 or 2
	 */
	@Column(length=10)
	long lastFetchAttempt;
	
	/**
	 * number of times we've tried to get the hash
	 */
	int fetchAttemptCount;
	
	@Column(length=10)
	long lastSeen;
	
	
	public int compareTo(TorrentDBEntry o) {
		for (int i = 0,n=info_hash.length; i < n; i++) {
			//needs & 0xFF since bytes are signed in Java
			//so we must convert to int to compare it unsigned
			int byte1 = info_hash[i] & 0xFF;
			int byte2 = o.info_hash[i] & 0xFF; 

			if (byte1 == byte2)
				continue;
			if (byte1 < byte2)
				return -1;
			return 1;
		}
		return 0;
	}
}
