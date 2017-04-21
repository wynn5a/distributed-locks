package io.github.wynn5a.redis;

import io.github.wynn5a.redis.client.JedisPoolBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by wynn5a
 */
public class TestRedisLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestRedisLock.class);
    private JedisPool pool;

    @Before
    private void setup() {
        LOGGER.debug("init jedis pool before test...");
        pool = new JedisPoolBuilder().create();
    }

    @Test
    public void testRedisClient() {
        try (Jedis jedis = pool.getResource()) {
            jedis.set("foo", "bar");
            String bar = jedis.get("foo");
            Assert.assertEquals(bar, "bar");

            jedis.del("foo");
            String barNull = jedis.get("foo");
            Assert.assertEquals(barNull, null);

            String result = jedis.set("lock", "random", "NX", "PX", 1000);
            Assert.assertEquals("OK", result);
            boolean locked = RedisLockUtils.lock("lock", "random", 1000, jedis);
            Assert.assertEquals(false, locked);
            RedisLockUtils.releaseLock("lock", "random", jedis);
            locked = RedisLockUtils.lock("lock", "random", 1000, jedis);
            Assert.assertEquals(true, locked);

            //clear data
            RedisLockUtils.releaseLock("lock", "random", jedis);
        }
    }

    @After
    public void cleanup() {
        LOGGER.debug("destroy jedis pool after test...");
        if (pool != null) {
            pool.destroy();
        }
    }

}
