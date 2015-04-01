# mldht

A java library implementing the bittorrent mainline DHT, with long-running server-class nodes in mind.

Originally developed as [DHT plugin](http://azsmrc.sourceforge.net/index.php?action=plugin-mldht) for Azureus/Vuze

## Features

- DHT spec itself ([BEP5](http://bittorrent.org/beps/bep_0005.html))
- IPv6 support ([BEP32](http://bittorrent.org/beps/bep_0032.html)) 
- Scrapes ([BEP33](http://bittorrent.org/beps/bep_0033.html)) 
- DHT security ([BEP42](http://bittorrent.org/beps/bep_0042.html)), partial: IP headers are implemented, node ID restrictions are not
- the following [libtorrent DHT extensions](http://www.libtorrent.org/dht_extensions.html): "get_peers response", "forward compatibility", "client identification"
- multihoming mode: separate IDs for each socket, shared routing table, automatically utilizes all available global unicast addresses available on the machine
- high-performance implementation without compromising correctness, i.e. the node will be a good citizen
 - 20k packets per second on a single Xeon core
- low latency lookups by using adaptive timeouts and a secondary routing table/cache tuned for RTT instead of stability
- export of passively observed \<infohash, ip\> tuples to redis to survey torrent activity
- fetch-only [metadata exchange (BEP9)](http://bittorrent.org/beps/bep_0009.html), code is there but currently unmaintained

## Dependencies

- java 8
- maven 3.1 (building)
- junit 4.x (tests)

## build:

    git clone https://github.com/the8472/mldht.git .
    mvn jar:jar

## run DHT node in standalone mode

    mkdir -p work
    cd work
    ../bin/run.sh


this will create various files
- `config.xml`, change settings as needed, core settings will be picked up on file modification
- `shutdown`, touch to cleanly shutdown running process (SIGHUP works too)
- `dht.cache`, used to skip bootstrapping after a restart
- `logs/*`, various diagnostics and log files

## network configuration

Stateful NATs or firewalls should be put into stateless mode/disable connection tracking and use static forwarding rules for the configured local ports [default: 49001]. Otherwise state table overflows may occur which lead to dropped packets and degraded performance.

Rules also should not assume any particular remote port, as other DHT nodes are free to chose their own. 

## launch with redis export

add the following lines to the `<components>` section of the config.xml:

    <component xsi:type="mldht:redisIndexerType">
      <className>the8472.mldht.PassiveRedisIndexer</className>
      <address>127.0.0.1</address>
    </component>

