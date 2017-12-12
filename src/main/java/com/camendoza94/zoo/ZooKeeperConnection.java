package com.camendoza94.zoo;


import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class ZooKeeperConnection {

    // Local Zookeeper object to access ZooKeeper ensemble
    private static ZooKeeper zoo;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);

    // Initialize the Zookeeper connection
    private ZooKeeperConnection() {
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = new FileInputStream("./src/main/resources/zookeeper.properties");
            prop.load(input);
            String host1 = prop.getProperty("server.host1");
            String port1 = prop.getProperty("server.port1");

            String host2 = prop.getProperty("server.host2");
            String port2 = prop.getProperty("server.port2");

            String host3 = prop.getProperty("server.host3");
            String port3 = prop.getProperty("server.port3");

            zoo = new ZooKeeper(host1 + ":" + port1 + "," + host2 + ":" + port2 + "," + host3 + ":" + port3  , 60000, we -> {
                if (we.getState() == KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            });
            connectionLatch.await();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    static ZooKeeper getZookeeperClient() {
        if (zoo == null || !zoo.getState().isAlive() ) {
            new ZooKeeperConnection();
        }

        return zoo;
    }

    // Method to disconnect from zookeeper server
    static void close() throws InterruptedException {
        zoo.close();
    }

}
