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
 * Created by fuwenming on 2017/4/17.
 */
public class TestRedisLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestRedisLock.class);
    private JedisPool pool;
    @Before
    public void setup(){
        LOGGER.debug("init jedis pool before test...");
        pool = new JedisPoolBuilder().connect("43.248.97.145", 6379).auth("test-1").timeout(10000).create();
    }

    @Test
    public void testRedisClient(){
        try(Jedis jedis = pool.getResource()){
            jedis.set("foo", "bar");
            String bar = jedis.get("foo");
            Assert.assertEquals(bar, "bar" );

            jedis.del("foo");
            String barNull = jedis.get("foo");
            Assert.assertEquals(barNull, null);
        }
    }

    @After
    public void clean(){
        LOGGER.debug("destroy jedis pool after test...");
        pool.destroy();
    }

}
