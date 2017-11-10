package com.camendoza94.zoo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClientManager implements ZooKeeperManager {

    private static ZooKeeper zooKeeper;

    private static ZooKeeperConnection zooKeeperConnection;


    ZooKeeperClientManager() {
        initialize();
    }


    private void initialize() {
        try {
            zooKeeperConnection = new ZooKeeperConnection();
            zooKeeper = zooKeeperConnection.connect("localhost");

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    void closeConnection() {
        try {
            zooKeeperConnection.close();
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void create(String path, byte[] data) throws KeeperException,
            InterruptedException {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

    }

    @Override
    public Stat getZNodeStats(String path) throws KeeperException,
            InterruptedException {
        Stat stat = zooKeeper.exists(path, true);
        if (stat != null) {
            System.out.println("Node exists and the node version is "
                    + stat.getVersion());
        } else {
            System.out.println("Node does not exists");
        }
        return stat;
    }

    @Override
    public Object getZNodeData(String path, boolean watchFlag) throws KeeperException,
            InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            Stat stat = getZNodeStats(path);
            byte[] b;
            if (stat != null) {
                if (watchFlag) {
                    b = zooKeeper.getData(path, we -> {

                        if (we.getType() == Watcher.Event.EventType.None) {
                            switch (we.getState()) {
                                case Expired:
                                    connectedSignal.countDown();
                                    break;
                            }
                        } else {
                            String path1 = "/MyFirstZnode";

                            try {
                                byte[] bn = zooKeeper.getData(path1,
                                        false, null);
                                String data = new String(bn,
                                        "UTF-8");
                                System.out.println(data);
                                connectedSignal.countDown();

                            } catch (Exception ex) {
                                System.out.println(ex.getMessage());
                            }
                        }
                    }, null);
                } else {
                    b = zooKeeper.getData(path, null, null);
                }

                String data = new String(b, "UTF-8");
                System.out.println(data);
                connectedSignal.await();
                return data;
            } else {
                System.out.println("Node does not exists");
                return null;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    @Override
    public void update(String path, byte[] data) throws KeeperException,
            InterruptedException {
        zooKeeper.setData(path, data, zooKeeper.exists(path, true).getVersion());

    }

    @Override
    public List<String> getZNodeChildren(String path) throws KeeperException,
            InterruptedException {
        Stat stat = getZNodeStats(path);
        List<String> children = null;

        if (stat != null) {
            children = zooKeeper.getChildren(path, false);
            for (String aChildren : children) System.out.println(aChildren);

        } else {
            System.out.println("Node does not exists");
        }
        return children;
    }

    @Override
    public void delete(String path) throws KeeperException,
            InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.delete(path, version);

    }
}