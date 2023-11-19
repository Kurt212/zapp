# Zapp - a high performant Key-Value Database

Zapp is a pure golang implemented kv database. It's very simple, but it's high efficient and allows building amazing systems on top of it.

And also this is a parody on all these modern pathos databases, which appear and die every day.

## Docs

- [Example code](./docs/examples/)
- [Zapp's Go Docs](https://pkg.go.dev/github.com/Kurt212/zapp)
- [Internals overview](./docs/internals.md). How Zapp works
- [Performance hints](./docs/perfomance_hints.md). How to make Zapp run faster
- [Performance tests](./docs/performance_test.md). Different usecases, and how they affect operations performance 

## Why this name

Zapp (database) is called after one of [the main Futurama's characters, Zapp Branigan](https://futurama.fandom.com/wiki/Zapp_Brannigan). Just like the prototype, the database is extremely efficient, fast, extremely sexy, smart, mature, space efficient, hardworking... aaaaaand... fast... aaaaand sexy... I'm not lying, I swear! You can watch the show yourself, if you don't believe me!

## How cool Zapp is?

First, it's very fast! Super fast! Here are the real performance testing numbers:
- Set - 1 338 284 QPS
- Get - 4 418 378 QPS
- Delete - 1 874 350 QPS

This is 50x times more than a single [PostgreSQL](https://www.postgresql.org/) can do!

Zapp is also fully ACID compatible! Check this out:
- Atomicity - we have transactions, which are executed and committed at once. We can not rollback them. No rollbacks - no problems!
- Consistency - we almost do not have any constraints! So your data model will always be consistent!
- Isolation - parallel requests are fully isolated from each other. In fact, Zapp provides serializable isolation level!
- Durability - you can enable Write Ahead Logging, and your data will be 100% safe!  

So, in fact, Zapp is much cooler, than any other database. It's more fast and has the same ACID features!

## I'm joking :)

Although this project is a joke, Zapp can be really used in production, if you need a durable performant key value storage. If you are not 100% sure, you are ready for this, then better use [Redis](https://redis.io/) or [PostgreSQL](https://www.postgresql.org/). 

Of course, in most cases you will not have millions QPS. Take a look at [real performance testing on an isolated VPS in Cloud](./docs/performance_test.md). If you want to learn more, how to tune performance, read the [Performance Tips Docs](./docs/perfomance_tips.md).

## Inspiration

Worth mentioning, that Zapp was inspired by another golang kv database [sniper](https://github.com/recoilme/sniper), which was really used in production. But there are differences: Zapp uses another collision resolving algorithm, Zapp provides data safety guarantees with the help of Write Ahead logging and etc.

## Mac OS tip
[How to Change Open Files Limit on OS X and macOS](https://gist.github.com/tombigel/d503800a282fcadbee14b537735d202c)

## Plans and Features

- [x] Creating segment's data file and filling it data
- [x] Selecting segment based on key hash value
- [x] Get, Set, Delete operations
- [x] Expiration time support. Expired keys are collected in background
- [x] Optional Write Ahead Logging. Each write operation generate a new WAL entry with a unique LSN.
- [x] Restoring Segment's data from WAL, if last applied LSN is lower, then the most recent LSN from WAL
- [x] Unit Test Coverage for most of the current Segment's logic. Perform a lot of testing for correctness
- [x] Write a performance testing code and make real performance testing on a VPS
- [ ] Write Docs and release to public
- [ ] Implement a mutable Min-Heap data structure inside Zapp to track items, which are about to expire. This is a replacement for an O(N) algorithm of checking each item in current collect-expired-items process
- [ ] Implement metrics reporting: performance, keys, dataset size, segments etc.
- [ ] Implement Zapp as a standalone daemon server with some standard Key-Value protocol. For example, [Memcached protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
- [ ] Implement a CLI application to manage Zapp daemon: restart, start, stop, metrics view, data access etc.
- [ ] Implement synchronous and asynchronous replication protocol for creating a cluster of Zapp with master-slave replication. Choose between logic and physic replication. The master pushes changes to sync replicas, async replicas pull changes from the master
- [ ] Implement the mechanism of promoting a slave instance into a master
- [ ] Implement a coordinator application with consensus algorithm inside for managing the cluster for automatic master failover
