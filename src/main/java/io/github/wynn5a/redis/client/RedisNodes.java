package io.github.wynn5a.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis nodes used for lock
 * <p>
 * Created by wynn5a on 2016/7/17.
 */
public class RedisNodes {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisNodes.class);
    private static final String HOST_PORT_SEPARATOR = ":";
    private static final int DEFAULT_TIMEOUT = 50;
    private List<Jedis> nodes = new ArrayList<>();

    /**
     * Every redis client should set a timeout period
     * <br/>
     * (smaller and much smaller than the total lock ttl time)
     *
     * @param timeout
     * @param nodeUris
     */
    public RedisNodes(int timeout, String... nodeUris) {
        for (String node : nodeUris) {
            String[] hostAndPortStrings = node.split(HOST_PORT_SEPARATOR);
            if (hostAndPortStrings.length == 2) {
                nodes.add(new Jedis(hostAndPortStrings[0], Integer.parseInt(hostAndPortStrings[1]), timeout));
            }
        }
        LOGGER.debug("init redis lock nodes : {}, timeout: {} ", nodes, timeout);
    }

    public RedisNodes(String... nodeUris) {
        this(DEFAULT_TIMEOUT, nodeUris);
    }

    public List<Jedis> getNodes() {
        return nodes;
    }

    public int getQuorum() {
        return nodes.size() / 2 + 1;
    }
}
