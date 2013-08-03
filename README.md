MeepoDB
=======

### Features
+ 100% sequential write to disk
+ 100% read from memory
+ Data stored in Cache-Oblivious Lookahead Array
+ Basic operations: GET, SET, DEL, DROP

### Limitations
+ Performance of sequential reads and writes is the same as random
+ No compression for keys and values
+ 128 B table name, 1 MiB single key, 1 GiB single value at most 

### Try It
<pre><code>$ cd path/to/meepodb
$ vi meepodb/config.go
$ make
$ ./meepodb-server 6631</code></pre>
<pre>$ cd path/to/meepodb
$ ./meepodb-cli<code></code></pre>

### Benchmark
CPU: Intel(R) Core(TM) i5 M450 @ 2.40GHz  
RAM: 6 GiB of DDR3 at 1067 MHz, 3 MiB of L3 cache  
ATA Disk: KingShare SSD032GAU (32 GB)  
File system: F2FS  
Key-value size: 16 bytes + 100 bytes (no compression)  
Read/write ops: 1,000,000  
Random reads: 199,643 ops/sec  
Random writes: 99,451 ops/sec  
