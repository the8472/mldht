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

import java.io.Serializable;

import javax.persistence.*;

import org.hibernate.annotations.ForeignKey;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.OptimisticLockType;


@Entity(name="scrapes")
public class ScrapeDBEntry implements Serializable {

	private static final long	serialVersionUID	= 7545132870970090363L;

	@Id
	@ManyToOne
	@JoinColumn(name="infoId", referencedColumnName="id")
	TorrentDBEntry torrent;
	
	@Id
	@Column(length=10)
	long created;

	int seeds;
	int leechers;
	int direct;
	
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ScrapeDBEntry)
		{
			ScrapeDBEntry other = (ScrapeDBEntry) obj;
			return other.torrent.equals(torrent) && created == other.created;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return (int) (created) + torrent.id;
	}
}
