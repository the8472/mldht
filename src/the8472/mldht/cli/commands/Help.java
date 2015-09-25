package the8472.mldht.cli.commands;

import the8472.mldht.cli.CommandProcessor;

public class Help extends CommandProcessor {

	@Override
	protected void process() {
		println("HELP                                - prints this help");
		println("PING ip port                        - continuously pings a DHT node with a 1 second interval");
		println("GETTORRENT infohash [infohash ...]  - peer lookup for <infohash(es)>, then attempt metadata exchange, then write .torrent file(s) to the current working directory");
		println("GETPEERS infohash [infohash ...]    - peer lookup for <infohash(es)>, print ip address/port tuples");
		println("BURST [count]                       - run a batch of find_node lookups to random target IDs. intended test the attainable throughput for active lookups, subject to internal throttling");
		exit(0);
	}

}
