package lbms.plugins.mldht.indexer;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity(name="ihdata")
public class TorrentDBEntry {
	
	@Id
	String info_hash;
	int status;
	long added;
	
	
}
