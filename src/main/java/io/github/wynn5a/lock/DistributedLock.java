package io.github.wynn5a.lock;

import java.util.concurrent.TimeUnit;

/**
 * Created by wynn5a on 2016/7/17.
 */
public interface DistributedLock {
    /**
     * never give up until get lock
     */
    void lock() throws InterruptedException;

    /**
     * try to get lock one time
     *
     * @return get lock success, true; fail false.
     */
    boolean tryLock();

    /**
     * try to get lock until timeout
     *
     * @param timeout
     * @param unit
     * @return success return true, fail return false
     */
    boolean tryLock(long timeout, TimeUnit unit);

    /**
     * just unlock
     */
    void unlock();
}
