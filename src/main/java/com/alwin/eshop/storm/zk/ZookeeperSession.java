package com.alwin.eshop.storm.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ZookeeperSession {

    private static final String DISTRIBUTE_LOCK_PATH = "/taskid-list-lock";
    private Logger logger = LoggerFactory.getLogger(ZookeeperSession.class);
    private static final CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zooKeeper;

    private ZookeeperSession() {
        try {
            this.zooKeeper = new ZooKeeper("192.168.31.11:2181," +
                    "192.168.31.12:2181,192.168.31.13:2181", 50000, new ZookeeperWatch());
            try {
                connectedSemaphore.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("ZooKeeper session established..........");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * 获取分布式锁
     */
    public void acquireDistributedLock() {
        try {
            zooKeeper.create(DISTRIBUTE_LOCK_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("success to acquire lock for {}", DISTRIBUTE_LOCK_PATH);
        } catch (Exception e) {
            // 如果该商品对应的锁的node已存在，即已经被别人加锁了，此时就会抛异常
            int count = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                    zooKeeper.create(DISTRIBUTE_LOCK_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e2) {
                    e2.printStackTrace();
                    count++;
                    continue;
                }
                logger.info("success to acquire lock for {} after {} times try......", DISTRIBUTE_LOCK_PATH, count);
                break;
            }
        }
    }

    /**
     * 释放分布式锁
     */
    public void releaseDistributedLock() {
        try {
            zooKeeper.delete(DISTRIBUTE_LOCK_PATH, -1);
            logger.info("release the lock for {}......", DISTRIBUTE_LOCK_PATH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createNode(String path) {
        try {
            zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {

        }
    }

    public String getNodeData(String path) {
        try {
            return new String(zooKeeper.getData(path, false, new Stat()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setNodeData(String path, String data) {
        try {
            zooKeeper.setData(path, data.getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class ZookeeperWatch implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            logger.info("Receive watched event: " + watchedEvent.getState());
            if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                connectedSemaphore.countDown();
            }
        }
    }

    private static class Singleton {
        private static ZookeeperSession instance;
        static {
            instance = new ZookeeperSession();
        }
        public static ZookeeperSession getInstance() {
            return instance;
        }
    }

    public static ZookeeperSession getInstance() {
        return Singleton.getInstance();
    }

}
