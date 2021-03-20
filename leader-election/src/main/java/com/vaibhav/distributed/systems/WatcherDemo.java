package com.vaibhav.distributed.systems;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher {

    private static final String ZOOKEEPER_ADDRESS =  "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target_znode";
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatcherDemo watcherDemo = new WatcherDemo();
        watcherDemo.connectToZookeeper();
        watcherDemo.watchTargetZnode();
        watcherDemo.run();
        watcherDemo.close();
        System.out.println("Disconnected from the zookeeper");
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS , SESSION_TIMEOUT, this);

    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) {
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
        System.out.println("Data : " + data + " children : " + children);

    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None -> {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully Connected");
                }else  {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
            }
            case NodeDeleted -> {
                System.out.println(TARGET_ZNODE + " was deleted") ;
            }
            case NodeCreated -> {
                System.out.println(TARGET_ZNODE + " was created ");
            }
            case NodeDataChanged ->  {
                System.out.println(TARGET_ZNODE + " data changed  ");
            }
            case NodeChildrenChanged -> {
                System.out.println(TARGET_ZNODE + " children  changed");
            }
        }

        try {
            watchTargetZnode();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
