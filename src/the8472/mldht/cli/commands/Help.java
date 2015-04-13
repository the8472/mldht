package the8472.mldht.cli.commands;

import the8472.mldht.cli.CommandProcessor;

public class Help extends CommandProcessor {

	@Override
	protected void process() {
		println("HELP                      - prints this help");
		println("PING ip port              - continuously pings a DHT node with a 1 second interval");
		println("GETTORRENT infohash path  - perform peer lookup for <infohash>, then attempt metadata exchange, then write .torrent to <path>");
		exit(0);
	}

}
