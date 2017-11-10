package com.camendoza94.zoo;


import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

class ZooKeeperConnection {

    // Local Zookeeper object to access ZooKeeper ensemble
    private ZooKeeper zoo;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);

    // Initialize the Zookeeper connection
    ZooKeeper connect(String host) throws IOException,
            InterruptedException {

        zoo = new ZooKeeper(host, 2000, we -> {

            if (we.getState() == KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });

        connectionLatch.await();
        return zoo;
    }

    // Method to disconnect from zookeeper server
    void close() throws InterruptedException {
        zoo.close();
    }

}
