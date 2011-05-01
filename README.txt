This source tree contains
1. A BitTorrent mainline DHT indexer
2. the mlDHT plugin for Azureus/Vuze

The readme is mostly for those who want to use the indexer.
If you found this and need help you probably know where to find me too (~ The_8472)


Building
--------

Directory structure for Eclipse, so building works with Eclipse .jardesc files. Might add shell scripts later.

Dependencies
------------

The indexer runs on hsqldb by default but if you add a mysql connector to the classpath that will work too simply by adjusting the hibernate.cfg

For other databases you'll have to write a custom Dialect to handle the index hints used internally

Azureus is only included for its bencoding libraries, other parts are not used.


Using data
----------

* the indexer downloads .torrents into ./torrents/
* the database indicates that .torrents have been downloaded by setting their "status" field to 2.
* the "lastSeen" column uses an EMA to track a smoothed time of incoming requests for an infohash. Assuming the system is spun up then "(unix_timestamp() - lastSeen)/33" gives you an averaged interval between incoming requests for each infohash 


Important:
	The indicated infohash by the database and the file names is the infohash under which it was advertised on the DHT. This may not be the correct infohash for the torrent, as faulty implementations may mangle the .torrent and thus derive an incorrect infohash and advertise it as such.
	Indicated to actual infohash mappings may be added to the database structure in the future.
	
	  


System Requirements
-------------------

* Oracle/Sun Java 1.6 or higher (1.7, once available)
* About 700MB to 2GB ram for the indexer and about 4GB ram if the database is to be cached in memory. 
* a multicore processor for good parallelism
* 200GB+ harddrive space to collect all the .torrents
* a system configuration allowing 200+ concurrent TCP connections, 8k+ UDP packets per second (10Mbit symmetrical) and 10k+ open file handles
* To achieve a good keyspace coverage of the DHT the indexer requires 20 or more IP addresses (for ipv4 and v6 each). It will bind to all publicly routeable addresses automatically

Actual resource usage depends on the number of IPs provided.

* the database engine must be ACI-compliant. D is not necessary. Due to highly concurrent access a table-locking database or not concurrency-safe one would be unsuitable.
* The database is used as state machine, when the indexer is not shut down cleanly objects may get stuck in a pending state, this can be cleaned up by issueing the following SQL statement:
	UPDATE ihdata set status = 0 where status = 1
	
Suggested sysctl config for linux (very high tcp connection churn is expected):
	net.ipv4.tcp_syn_retries=2
	
	
Setup for mysql
---------------

* Mysql 5.5+ is recommended
* the mysql/j connector jar has to be placed in the classpath, e.g. the libs/ dir
* following my.cnf entries are suggested for good performance:
	[mysqld]
	innodb_buffer_pool_size = 4G
	innodb_flush_log_at_trx_commit = 1
	innodb_log_file_size = 256M

* modify the following properties in the hibernate.cfg.xml:

                <property name="connection.url">jdbc:mysql://localhost/YOUR DATABASE HERE</property> <!-- ?useCursorFetch=true&amp;useServerPrepStmts=true -->
                <property name="connection.username">YOUR USERNAME HERE</property>
                <property name="connection.password">YOUR PASSWORD HERE</property>
                <property name="dialect">lbms.plugins.mldht.indexer.db.MySQLIdxHintDialect</property>
                <property name="hbm2ddl.auto">update</property>

* create the table in your database (the table structure may change in the future, consult this readme if you update from SVN):

	CREATE TABLE `ihdata` (
	  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
	  `info_hash` binary(20) NOT NULL,
	  `hitCount` int(10) unsigned NOT NULL DEFAULT '0',
	  `status` tinyint(3) unsigned NOT NULL DEFAULT '0',
	  `lastLookupTime` int(10) unsigned NOT NULL DEFAULT '0',
	  `fetchAttemptCount` int(10) unsigned NOT NULL DEFAULT '0',
	  `indexAttemptCount` tinyint(10) unsigned NOT NULL DEFAULT '0',
	  `added` int(10) unsigned NOT NULL DEFAULT '0',
	  `lastSeen` int(10) unsigned NOT NULL DEFAULT '0',
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `infohashIdx` (`info_hash`),
	  KEY `statusIdx` (`status`)
	) ENGINE=InnoDB DEFAULT CHARSET=latin1

	CREATE TABLE `scrapes` (
		`infoId` INT(10) UNSIGNED NOT NULL,
		`created` INT(10) UNSIGNED NOT NULL,
		`seeds` INT(10) UNSIGNED NOT NULL,
		`leechers` INT(10) UNSIGNED NOT NULL,
		`overall` INT(10) UNSIGNED NOT NULL,
		PRIMARY KEY (`infoId`, `created`),
		CONSTRAINT `FK__ihdata` FOREIGN KEY (`infoId`) REFERENCES `ihdata` (`id`) ON UPDATE RESTRICT ON DELETE RESTRICT
	)
	ENGINE=InnoDB

