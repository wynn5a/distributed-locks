package io.github.wynn5a.zookeeper.impl;

import io.github.wynn5a.lock.DistributedLock;
import io.github.wynn5a.zookeeper.ZooKeeperLockConstant;
import io.github.wynn5a.zookeeper.client.ZooKeeperClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by fuwenming
 */
public class ZookeeperLock implements DistributedLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLock.class);
    private ZooKeeperClient client;
    private String basePath = "/ZOOKEEPER_BASED_LOCK";
    private String resource = "LOCK";
    private static final long DEFAULT_RETRY_WAITING_TIME = 500;
    private static AtomicBoolean GOT = new AtomicBoolean(false);
    private String lockedNode;

    public ZookeeperLock(ZooKeeperClient client, String resource) {
        if (StringUtils.isBlank(resource)) {
            throw new IllegalArgumentException("Locked resource name cannot be empty!");
        }
        this.client = client;
        this.resource = resource;
        client.createIfNotExist(basePath);
    }


    @Override
    public void lock() throws InterruptedException {
        tryLock(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock() {
        LOGGER.info("try to get lock once for resource: {}", resource);
        //make sure path of lock is exists
        client.createIfNotExist(getLockPath());

        //try to acquire lock
        String path = client.createLockNode(getLockPath());
        LOGGER.info("Lock node is created, path: {}", path);
        if (StringUtils.isBlank(path) || !path.contains(getLockPath())) {
            return false;
        }
        //check if get lock
        String nodeName = getNodeName(path);
        LOGGER.info("create lock node : {}", nodeName);
        boolean result = client.checkIfGot(nodeName, getLockPath());
        if (!result) {
            //fail to get, quit and delete lock node
            client.deleteLockNode(getLockPath(), nodeName);
            client.closeConnection();
        } else {
            lockedNode = nodeName;
            LOGGER.info("Lock got, current locked node is {}", lockedNode);
        }
        return result;
    }

    private String getNodeName(String path) {
        return path.substring(getLockPath().length() + 1, path.length());
    }

    private String getLockPath() {
        return basePath + ZooKeeperLockConstant.PATH_SEPARATOR + resource;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        LOGGER.info("try to get lock for resource: {} until timeout: {} {}", resource, timeout, unit.name());
        //make sure path of lock is exists
        client.createIfNotExist(getLockPath());
        CountDownLatch latch = new CountDownLatch(1);
        String nodeName = "";
        long wait = timeout == -1 ? -1 : unit.toMillis(timeout);
        long start = System.currentTimeMillis();
        try {
            if (StringUtils.isBlank(nodeName)) {
                while (wait == -1 || System.currentTimeMillis() - start < wait) {
                    //try to acquire lock
                    String path = client.createLockNode(getLockPath());
                    if (StringUtils.isBlank(path) || !path.contains(getLockPath())) {
                        Thread.currentThread().sleep(DEFAULT_RETRY_WAITING_TIME);
                        continue;
                    }
                    nodeName = getNodeName(path);
                    LOGGER.info("create lock node : {}", nodeName);
                    wait = wait == -1 ? -1 : (wait - (System.currentTimeMillis() - start));
                    break;
                }
                if (StringUtils.isBlank(nodeName)) {
                    return false;
                }
            }
            boolean result = client.checkAndWatch(retryWatcher(nodeName, latch, client), nodeName, getLockPath());
            if (!result) {
                GOT.set(false);
                if (wait == -1) {
                    latch.await();
                } else {
                    latch.await(wait, TimeUnit.MILLISECONDS);
                }
                if (!GOT.get()) {
                    client.deleteLockNode(getLockPath(), nodeName);
                    client.closeConnection();
                } else {
                    lockedNode = nodeName;
                    LOGGER.info("Lock got, current locked node is {}", lockedNode);
                }
            } else {
                lockedNode = nodeName;
                LOGGER.info("Lock got, current locked node is {}", lockedNode);
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Exception happened!", e);
            if (!GOT.get() && !client.isClosed()) {
                client.closeConnection();
            }
        }
        return GOT.get();
    }

    @Override
    public void unlock() {
        if (StringUtils.isNotEmpty(lockedNode)) {
            client.deleteLockNode(getLockPath(), lockedNode);
        }
        client.closeConnection();
    }

    private Watcher retryWatcher(final String nodeName, CountDownLatch latch, ZooKeeperClient client) {
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    boolean result = client.checkAndWatch(this, nodeName, getLockPath());
                    if (result) {
                        GOT.set(true);
                        latch.countDown();
                    }
                } catch (Exception e) {
                    LOGGER.error("Get event : {} and exception happened!", event.getType().name(), e);
                }
            }
        };
    }
}
