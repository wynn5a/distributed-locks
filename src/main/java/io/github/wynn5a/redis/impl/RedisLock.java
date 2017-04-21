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
 * Implement a simple version distributed lock based on <a href="https://redis.io/topics/distlock">REDLOCK</a> algorithm
 * <p>
 * Created by fuwenming
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
