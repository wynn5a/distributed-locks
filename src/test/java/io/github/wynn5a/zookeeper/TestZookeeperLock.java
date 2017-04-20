package io.github.wynn5a.zookeeper;

import io.github.wynn5a.zookeeper.client.ZooKeeperClient;
import io.github.wynn5a.zookeeper.impl.ZookeeperLock;
import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by fuwenming on 2016/4/21.
 */
public class TestZookeeperLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestZookeeperLock.class);
    private ZooKeeper zooKeeper;

    public void setup() {
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2183", 100000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    LOGGER.info("Get event : {}", event.getType().getIntValue());
                }
            });
        } catch (IOException e) {
            LOGGER.error("init zookeeper client error", e);
        }
    }

    @Test
    public void testZkClient() {
        try {
            setup();
            zooKeeper.create("/test/test-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            List<String> nodes = zooKeeper.getChildren("/test", false);
            Assert.assertTrue(nodes.contains("Test"));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLock() throws InterruptedException {
        String resource = "Lock";
        new Thread(() -> {
            ZookeeperLock lock = new ZookeeperLock(new ZooKeeperClient("127.0.0.1:2183"), resource);
            try {
                lock.lock();
                Thread.currentThread().sleep(5000);
                lock.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        Thread.currentThread().sleep(1000);

        new Thread(() -> {
            ZookeeperLock lock = new ZookeeperLock(new ZooKeeperClient("43.248.97.145:2183"), resource);
            Assert.assertFalse(lock.tryLock());
        }).start();

        Thread.currentThread().sleep(5 * 1000);

        new Thread(() -> {
            ZookeeperLock lock = new ZookeeperLock(new ZooKeeperClient("43.248.97.145:2183"), resource);
            Assert.assertTrue(lock.tryLock());
        }).start();

        Thread.currentThread().sleep(5 * 1000);
    }

    @After
    public void cleanup() throws InterruptedException {
        if (zooKeeper != null)
            zooKeeper.close();
    }
}
