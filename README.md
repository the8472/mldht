# mldht

A java library implementing the bittorrent mainline DHT, with long-running server-class nodes in mind.

Originally developed as [DHT plugin](http://azsmrc.sourceforge.net/index.php?action=plugin-mldht) for Azureus/Vuze

## Features

- DHT spec itself ([BEP5](http://bittorrent.org/beps/bep_0005.html))
- IPv6 support ([BEP32](http://bittorrent.org/beps/bep_0032.html)) 
- Scrapes ([BEP33](http://bittorrent.org/beps/bep_0033.html)) 
- DHT security ([BEP42](http://bittorrent.org/beps/bep_0042.html)), partial: IP headers are implemented, node ID restrictions are not
- metadata exchange ([BEP9](http://bittorrent.org/beps/bep_0009.html)), fetch-only
- the following [libtorrent DHT extensions](http://www.libtorrent.org/dht_extensions.html): "get_peers response", "forward compatibility", "client identification"
- optional multihoming mode: separate IDs for each socket, shared routing table, automatically utilizes all available global unicast addresses available on the machine
- high-performance implementation without compromising correctness, i.e. the node will be a good citizen
 - can process 20k packets per second on a single Xeon core
- low latency lookups by using adaptive timeouts and a secondary routing table/cache tuned for RTT instead of stability
- export of passively observed \<infohash, ip\> tuples to redis to survey torrent activity

## Dependencies

- java 8
- maven 3.1 (building)
- junit 4.x (tests)

## build

    git clone https://github.com/the8472/mldht.git .
    mvn jar:jar

## run DHT node in standalone mode

    mkdir -p work
    cd work
    ../bin/mldht-daemon
    
this will create various files
- `config.xml`, change settings as needed, core settings will be picked up on file modification
- `shutdown`, touch to cleanly shutdown running process (SIGHUP works too)
- `dht.cache`, used to skip bootstrapping after a restart
- `logs/*`, various diagnostics and log files

**Security note:** the shell script launches the JVM with a debug port bound to localhost for easier maintenance, thus allowing arbitrary code execution with the current user's permissions. In a multi-user environment a custom script with debugging disabled should be used    

## embedding as library

It is not necessary to use the standalone [<tt>Launcher</tt>](src/the8472/mldht/Launcher.java), instead you can create [<tt>DHT</tt>](src/lbms/plugins/mldht/kad/DHT.java) instances and control their configuration and lifecycle directly.

Consider the Launcher as an example-case how to instantiate DHT nodes.


## network configuration

* stateful NATs or firewalls should be put into stateless mode/disable connection tracking and use static forwarding rules for the configured local ports [default: 49001].<br>Otherwise state table overflows may occur which lead to dropped packets and degraded performance.
* nat/firewall rules should not assume any particular remote port, as other DHT nodes are free to chose their own.
* If no publicly routable IPv6 address is available then IPv6 should be disabled
* If only NATed IPv4 addresses are available then multihoming mode should be disabled
* The length of network interface send queues should be increased when the DHT node is operated in multihoming mode on a server with many public IPs.<br>This is necessary because UDP sends may be silently dropped when the send queue is full and DHT traffic can be very bursty, easily saturating too-small queues<br>Check system logs or netstat statistics to see if outgoing packets are dropped.
* For similar reasons the maximum socket receive buffer size should be set to at least 2MB, which is the amount this implementation will request when configuring its sockets

## launch with redis export

add the following lines to the `<components>` section of the config.xml:

```xml
    <component xsi:type="mldht:redisIndexerType">
      <className>the8472.mldht.PassiveRedisIndexer</className>
      <address>127.0.0.1</address>
    </component>
```

## remote-cli component

launch daemon with

```xml
    <component>
      <className>the8472.mldht.cli.Server</className>
    </component>
```

run CLI client with

```
bin/mldht-remote-cli help
```

**Security note:** The CLI Server component listens on localhost, accepting commands without authentication from any user on the system. It is recommended to not use this component in a multi-user environment. 


## launching custom components

Implement [<tt>Component</tt>](src/the8472/mldht/Component.java) and configure the launcher to include it on startup through the config.xml:
	
```xml
    <component>
      <className>your.class.name.Here</className>
    </component>
```