package com.camendoza94.zoo;


import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

class ZooKeeperConnection {

    // Local Zookeeper object to access ZooKeeper ensemble
    private static ZooKeeper zoo;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);

    // Initialize the Zookeeper connection
    private ZooKeeperConnection() {

        try {
            zoo = new ZooKeeper("localhost", 6000, we -> {

                if (we.getState() == KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            });
            connectionLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    static ZooKeeper getZookeeperClient() {
        if (zoo == null) {
            new ZooKeeperConnection();
        }
        return zoo;
    }

    // Method to disconnect from zookeeper server
    static void close() throws InterruptedException {
        zoo.close();
    }

}
