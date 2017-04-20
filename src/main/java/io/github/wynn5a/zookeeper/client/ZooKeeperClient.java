package io.github.wynn5a.zookeeper.client;

import io.github.wynn5a.zookeeper.ZooKeeperLockConstant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by fuwenming on 2016/4/20.
 */
public class ZooKeeperClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClient.class);
    private static final int DEFAULT_TIME_OUT = 60 * 1000;
    private ZooKeeper zooKeeper;
    private AtomicBoolean closed = new AtomicBoolean(true);

    public ZooKeeperClient(String connectionString) {
        this(connectionString, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                LOGGER.info("zookeeper client get event:{}", event.getType().name());
            }
        }, DEFAULT_TIME_OUT, 0);
    }

    public ZooKeeperClient(String connectionString, Watcher watcher, int timeout) {
        this(connectionString, watcher, timeout, 0);
    }

    public ZooKeeperClient(String connectionString, Watcher watcher, int timeout, int retryCount) {
        do {
            try {
                zooKeeper = new ZooKeeper(connectionString, timeout, watcher);
                closed.set(false);
                break;
            } catch (IOException e) {
                LOGGER.error("network failure to connect : {}", connectionString);
                retryCount--;
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        } while (retryCount > 0);
    }

    public boolean createIfNotExist(String path) {
        try {
            Stat exist = zooKeeper.exists(path, false);
            if (exist != null) {
                LOGGER.info("Path of lock: {} is exists!", path);
                return true;
            } else {
                LOGGER.info("Path of lock: {} will be created!", path);
                zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Path of lock: {} is created!", path);
                return false;
            }
        } catch (KeeperException e) {
            LOGGER.error("Exception happened!", e);
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted!", e);
        }
        return false;
    }

    public String createLockNode(String lockPath) {
        try {
            return zooKeeper.create(lockPath + ZooKeeperLockConstant.PATH_SEPARATOR + ZooKeeperLockConstant.LOCK_NODE_NAME_PREFIX, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            LOGGER.error("Exception happened!", e);
        }
        return "";
    }

    public boolean checkIfGot(String nodeName, String lockPath) {
        try {
            List<String> children = zooKeeper.getChildren(lockPath, false);
            Collections.sort(children);
            return nodeName.equals(children.get(0));
        } catch (Exception e) {
            LOGGER.error("Exception happened when check getting the lock or not", e);
        }
        return false;
    }

    public void closeConnection() {
        try {
            zooKeeper.close();
            closed.set(true);
        } catch (InterruptedException e) {
            LOGGER.error("Exception happened!", e);
        }
    }

    public boolean checkAndWatch(Watcher watcher, String nodeName, String lockPath) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(lockPath, false);
        Collections.sort(children);
        int indexOfMe = children.indexOf(nodeName);
        if (indexOfMe == 0) {
            return true;
        }
        LOGGER.info("Watcher will be set for node name: {} of lock: {}", nodeName, lockPath);
        Stat stat = zooKeeper.exists(lockPath + ZooKeeperLockConstant.PATH_SEPARATOR + children.get(indexOfMe - 1), watcher);
        return stat == null && checkIfGot(nodeName, lockPath);
    }

    public void deleteLockNode(String lockPath, String nodeName) {
        try {
            LOGGER.debug("Node name: {} of base path: {} will be deleted!", nodeName, lockPath);
            zooKeeper.delete(lockPath + ZooKeeperLockConstant.PATH_SEPARATOR + nodeName, 0);
        } catch (Exception e) {
            LOGGER.error("Exception happened!", e);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }
}
