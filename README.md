# distributed-locks

Distributed locking based on zookeeper(ZooKeeperLock) and Redis(RedisLock)

## ZooKeeperLock
 Implement based on the [recipes](https://zookeeper.apache.org/doc/trunk/recipes.html) of ZooKeeper.

 Usage:

```Java

 ZookeeperLock lock = new ZookeeperLock(new ZooKeeperClient("127.0.0.1:2183"), resource);
 try {
   lock.lock();
   Thread.currentThread().sleep(5000);
   lock.unlock();
 } catch (InterruptedException e) {
   e.printStackTrace();
 }

```

## RedisLock
Implement based on [REDLOCK](https://redis.io/topics/distlock) algorithm.

Usage:

```Java
RedisLock lock = new RedisLock(new RedisNodes("127.0.0.1:3679", "127.0.0.1:3678","127.0.0.1:3677"),"LOCK", 60000);
try {
    lock.lock();
    doingJob();
    lock.unlock();
} catch (Exception e) {
    //doing something
}

```
