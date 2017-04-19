package io.github.wynn5a.redis.impl;

import io.github.wynn5a.lock.DistributedLock;
import io.github.wynn5a.redis.RedisLockUtils;
import io.github.wynn5a.redis.client.RedisNodes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 1.获得以毫秒为单位的当前时间。
 * 2.试图顺序获取锁在所有的N个节点上,在所有实例上使用相同的Key和随机值。
 * 在步骤2,在每个实例中设置锁时，客户端使用一个超时（比总的自动释放锁时间小）获得。
 * 例如如果自动释放锁时间是10秒,超时可能为 5-50毫秒范围内。
 * 这可以防止客户端很长时间试图跟一个不可用的Redis节点保持连接。
 * 如果一个实例不可用,我们应该尽快连接下一个实例。
 * 3.客户端计算为了获得锁使用的时间，通过当前时间减去在步骤1中获得的时间戳。
 * 成功获得锁的因素是：当且仅当客户端能够获得大多数（N / 2 + 1）实例（至少3个）的锁，
 * 并且总的使用时间小于锁有效时间。
 * 4.如果获取到了锁，其有效时间为初始化的有效时间减去步骤3中计算得到的时间。
 * 5.如果客户未能获取锁,由于某种原因(无法获取N / 2 + 1实例的锁或有效时间为负),
 * 它将试图释放所有实例(即使实例认为这是无效的锁)的锁。
 * <p>
 * Implement a simple version distributed lock based on <a href="https://redis.io/topics/distlock">REDLOCK</a> algorithm
 * <p>
 * Created by fuwenming on 2016/7/19.
 */
public class RedisLock implements DistributedLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisLock.class);
    private static final int DEFAULT_LOCK_TTL = 10 * 1000;//10 seconds
    private static final int DEFAULT_WAITING_TIME = 1000;
    private static final String DEFAULT_LOCK_KEY = "REDIS_BASED_LOCK";
    private static final double CLOCK_DRIFT_FACTOR = 0.01;

    private long ttl = DEFAULT_LOCK_TTL;
    private RedisNodes nodes;
    private String resource = DEFAULT_LOCK_KEY;
    private String randomValue;
    private long lockValidity = 0;

    public RedisLock(RedisNodes nodes, String resource, long ttl) {
        this.ttl = ttl;
        this.resource = resource;
    }

    public RedisLock(RedisNodes nodes, long ttl) {
        this.ttl = ttl;
        this.nodes = nodes;
    }

    public RedisLock(RedisNodes nodes) {
        this.nodes = nodes;
    }

    /**
     * @see DistributedLock#lock()
     */
    @Override
    public void lock() throws InterruptedException {
        while (!tryLock()) {
            Thread.currentThread().sleep(DEFAULT_WAITING_TIME);
        }
    }

    /**
     * @return return <pre>true</pre> if get lock success, else return <pre>false</pre>
     * @see DistributedLock#tryLock()
     */
    @Override
    public boolean tryLock() {
        getRandomValue();
        int lockedNodes = 0;
        long begin = System.currentTimeMillis();
        for (Jedis node : nodes.getNodes()) {
            if (RedisLockUtils.lock(resource, randomValue, ttl, node)) {
                lockedNodes++;
            }
        }
        //Add 2 milliseconds to the drift to account for Redis expires
        // precision, which is 1 millisecond, plus 1 millisecond min drift
        // for small TTLs.
        int drift = (int) (ttl * CLOCK_DRIFT_FACTOR) + 2;
        long cost = System.currentTimeMillis() - begin + drift;
        boolean result = (lockedNodes >= nodes.getQuorum()) && (cost < ttl);
        if (!result) {
            for (Jedis node : nodes.getNodes()) {
                RedisLockUtils.releaseLock(resource, randomValue, node);
            }
        } else {
            lockValidity = ttl - cost;
        }
        return result;
    }

    private void getRandomValue() {
        if (StringUtils.isBlank(randomValue)) {
            randomValue = Thread.currentThread().getId() + ":" + UUID.randomUUID().toString();
        }
    }

    /**
     * @param timeout
     * @param unit
     * @return return <pre>true</pre> if get lock success, else return <pre>false</pre>
     * @see DistributedLock#tryLock(long, TimeUnit)
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        long start = System.currentTimeMillis();
        long totalWaiting = unit.toMillis(timeout);
        while (!tryLock()) {
            if (System.currentTimeMillis() - start >= totalWaiting) {
                return false;
            }
            try {
                Thread.currentThread().sleep(DEFAULT_WAITING_TIME);
            } catch (InterruptedException e) {
                LOGGER.error("Current thread is interrupted and ignored by lock.");
            }
        }
        return true;
    }

    @Override
    public void unlock() {
        for (Jedis node : nodes.getNodes()) {
            RedisLockUtils.releaseLock(resource, randomValue, node);
        }
    }

    public long getValidity() {
        return lockValidity;
    }
}
