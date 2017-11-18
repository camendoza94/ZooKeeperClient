package com.camendoza94.zoo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClientManager implements ZooKeeperManager {

    private static ZooKeeper zooKeeper;


    public ZooKeeperClientManager() {
        initialize();
    }


    private void initialize() {
        try {
            zooKeeper = ZooKeeperConnection.getZookeeperClient();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    void closeConnection() {
        try {
            ZooKeeperConnection.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
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
            System.out.println("Node does not exist");
        }
        return stat;
    }

    @Override
    public int getZNodeData(String path, boolean watchFlag) throws KeeperException,
            InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            Stat stat = getZNodeStats(path);
            byte[] b;
            if (stat != null) {
                if (watchFlag) {
                    connectedSignal.await();
                    b = zooKeeper.getData(path, we -> {

                        if (we.getType() == Watcher.Event.EventType.None) {
                            switch (we.getState()) {
                                case Expired:
                                    connectedSignal.countDown();
                                    break;
                            }
                        } else {

                            try {
                                byte[] bn = zooKeeper.getData(path,
                                        false, null);
                                Integer data = (int) bn[0];
                                System.out.println(data);
                                connectedSignal.countDown();

                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }, null);
                } else {
                    b = zooKeeper.getData(path, null, null);
                }

                Integer data = (int) b[0];
                System.out.println(data);
                connectedSignal.countDown();
                return data;
            } else {
                System.out.println("Node does not exist");
                return -1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
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
        List<String> children = new ArrayList<>();

        if (stat != null) {
            children = zooKeeper.getChildren(path, false);
            for (String aChildren : children) System.out.println(aChildren);

        } else {
            System.out.println("Node does not exists");
        }
        return children;
    }

    public List<String> getZNodeTree(String root) throws KeeperException, InterruptedException {
        List<String> paths = new ArrayList<>();
        traverseTree(root, "", paths);
        return paths;
    }

    private void traverseTree(String root, String path, List<String> paths) throws KeeperException, InterruptedException {
        path += "/" + root;

        List<String> children = getZNodeChildren(path);
        if (children.isEmpty()) {
            paths.add(path);
        } else {
            for (String child : children) {
                traverseTree(child, path, paths);
            }
        }
    }

    @Override
    public void delete(String path) throws KeeperException,
            InterruptedException {
        int version = zooKeeper.exists(path, true).getVersion();
        zooKeeper.delete(path, version);

    }

    void multi(List<Op> ops) throws KeeperException, InterruptedException {
        zooKeeper.multi(ops);
    }
}