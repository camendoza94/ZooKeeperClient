/*
 * @(#)ZKManager.java
 * @author Binu George
 * Globinch.com
 * copyright http://www.java.globinch.com. All rights reserved.
 */
package com.camendoza94.zoo;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public interface ZooKeeperManager {
    /**
     * Create a Znode and save some data
     */
    void create(String path, byte[] data) throws KeeperException,
            InterruptedException;

    /**
     * Get the ZNode Stats
     */
    Stat getZNodeStats(String path) throws KeeperException,
            InterruptedException;

    /**
     * Get ZNode Data
     */
    Object getZNodeData(String path,boolean watchFlag) throws KeeperException,
            InterruptedException;

    /**
     * Update the ZNode Dat
     */
    void update(String path, byte[] data) throws KeeperException,
            InterruptedException;

    /**
     * Get ZNode children

     */
    List<String> getZNodeChildren(String path) throws KeeperException,
            InterruptedException;

    /**
     * Delete the znode
     */
    void delete(String path) throws KeeperException,
            InterruptedException;
}