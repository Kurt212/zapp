# Performance Tips

## Drive bounded system by design

Zapp is designed to store big amounts of key-value pairs. It can store dozens or even hundreds of millions of pairs. Zapp sacrifices some performance due to low memory usage. It doesn't keep item's keys in memory, because they have variable length and may require a lot of space. Instead, it stores keys on a drive and each time reads them to compare with the needed key.

Zapp is an IO-bounded system, because it makes several read and write operations per request. GET operation has to find the needed item on the drive by sequentially reading all items with the same hash value. Write has to find the old item with the same key and mark it as deleted, before writing new item to the drive. Delete operation does the same: finds the value and marks it as deleted.

## Drive performance dependencies

How fast Zapp serves your requests depends on how you use it. It's not a silver bullet and there are a lot of cases, when you can choose another Database and achieve more performance.

1. Zapp fully relies on OS's filesystem caching mechanism. When reading and writing to Data File the OS writes and reads from special buffers in RAM. These buffers is just a cache, which OS creates to reduce real IO operations. The buffers are synced to the real device in background implicitly by the OS or explicitly by the program with the [fsync syscall](https://en.wikipedia.org/wiki/Sync_(Unix)).

2. Zapp has a background process of syncing data to the drive explicitly. It's a periodic task executed on timer. Calling Sync is a very expensive operation, it blocks execution of incoming requests. So the more often the Sync is called, the less performant your system will be. On the other hand, syncing data is important, if you are afraid to loose data. So this is a good parameter for tuning.

3. The first thing you should know is that OS tries to cache as much data as can fit in RAM. When there's not enough memory to cache all data, OS starts to read directly from the hardware device and continues caching only hot pages. So adding more RAM can boost your performance dramatically. 

4. The OS filesystem caching performance depends on whether the data is stored in memory, or it's too big to fit. So the length of your keys and values matters. The smaller data you store - the more chance it is cached by OS.

5. The important hardware drive parameters are both the write and read bandwidth (MB/s) and the IO write and read operations (IO/s). If your data is large, then the performance will be probably limited by bandwidth. If your data is pretty small, then the performance will be limited by the number of IO operations per second. 

6. At last, your pattern of accessing data is important, if not all data fits in RAM. If you are not lucky, you will run into a lot of cache misses. A cache miss happens, when OS has no cache for that particular part and it accesses the real hardware drive to read or to write. So basically, if your requests are, for example, [normally distributed](https://en.wikipedia.org/wiki/Normal_distribution), then your performance will be better.

## Vertical scaling

The idea is simple. You can add more drives to partition data over them. Parallel requests will access different hardware and will not be limited by single drive performance. 

Another simple idea is to add more RAM to be able to cache more data and speed up the requests serving.

Add more CPUs to the server, so that you can run execute more requests in parallel.


## Enabling WAL

Having an enabled/disabled WAL feature is a choice between performance and durability. Zapp provides a switch to turn WAL logging logic on. By default, Zapp has this featured turned off.

Zapp without WAL doesn't guarantee, that your data will be safe, if some failure happens in runtime. For example, a sudden electricity blackout can cause your computer to turn off without gracefully terminating Zapp. Some of the data, which was not synced to the drive, would be lost. But if the WAL feature is enabled, then Zapp will safely restore all the data by reapplying the actions in the right same order from the WAL File.

Having WAL is not cheap. Each modifying operation (Set or Delete) requires appending new entry to the WAL and syncing the WAL file to the drive. Appending to a file is a more performant operation, than writing at random offset, but still this is a synchronous drive operation.

Enabling WAL can reduce your modify requests by 2-10x times, depending on your usecase. But your read requests will not suffer. Get operations will have the exact same performance, because it doesn't require appending to WAL. So, if you are okay with slow writes or you have much more reads then writes, then enabling WAL will not cause any pain.

## The best and the worst use case

In conclusion, let's image how the most performant and the lest performant setups would look like.

### The best case

If I follow all the tuning performance tips, then I would set up a separate server with a lot of fast RAM for good caching and several independent SSD drives to split data over them. I would disable WAL feature. And I would make the sync process run not too often. My data length would be small enough.

### The worst case

Well, I want to make Zapp work slow as hell, then I would run Zapp on a computer which runs several other IO based processes. There would be not much RAM, and I would use an old simple Hard Drive, not a modern SSD. I would make my dataset very large so it doesn't fit RAM for sure. And, of course, I would access my data very randomly so that I will always run into a cache-miss.
