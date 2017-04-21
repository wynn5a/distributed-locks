package io.github.wynn5a.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Collections;

/**
 * Created by wynn5a
 */
public class RedisLockUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisLockUtils.class);
    private static final String OK = "OK|+OK";
    private static final String RELEASE_SCRIPT = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then " +
            "	return redis.call(\"del\",KEYS[1]) " +
            "else " +
            "	return 0 " +
            "end ";

    /**
     * try to acquired lock in redis node
     * <p>
     * <pre>SET resource_name my_random_value NX PX 3000 <pre/>
     *
     * @param resource
     * @param randomValue
     * @param ttl
     * @param node
     * @return
     */
    public static boolean lock(String resource, String randomValue, long ttl, Jedis node) {
        try {
            String result = node.set(resource, randomValue, "NX", "PX", ttl);
            return OK.contains(result);
        } catch (Exception e) {
            LOGGER.info("Exception: {} happened when lock resource: {}", e.getMessage(), resource);
            return false;
        }
    }

    public static void releaseLock(String resource, String randomValue, Jedis node) {
        try {
            node.eval(RELEASE_SCRIPT, Collections.singletonList(resource), Collections.singletonList(randomValue));
        } catch (Exception e) {
            LOGGER.error("Exception happened when release lock: {}", e.getMessage());
        }
    }
}
