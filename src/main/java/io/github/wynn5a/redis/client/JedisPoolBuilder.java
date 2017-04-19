package io.github.wynn5a.redis.client;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * To create a Jedis pool easilier by using default value
 * <p>
 * Created by wynn5a on 2016/7/17.
 */
public class JedisPoolBuilder {
    private String host = "localhost";
    private int port = 6379;
    private String authorization;
    private JedisPoolConfig config = new JedisPoolConfig();
    private int timeout = 3000;

    public JedisPoolBuilder connect(String host, int port) {
        this.host = host;
        this.port = port;
        return this;
    }

    public JedisPoolBuilder bind(int port) {
        this.port = port;
        return this;
    }

    public JedisPoolBuilder config(JedisPoolConfig config) {
        this.config = config;
        return this;
    }

    public JedisPoolBuilder auth(String authorization) {
        this.authorization = authorization;
        return this;
    }

    public JedisPoolBuilder timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public JedisPool create() {
        return new JedisPool(config, host, port, timeout, authorization);
    }
}
