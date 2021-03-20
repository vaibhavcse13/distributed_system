package com.vaibhav.distributed.systems;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection  implements Watcher  {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private  static  final  int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    

    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws InterruptedException, KeeperException {

        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.relcetLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from the zookeeper, exiting from the application");
    }

    public void run() throws  InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    // Volunteer for leadership
    public void volunteerForLeadership() throws KeeperException , InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name" + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/" , "");
        
    }

    public void relcetLeader() throws KeeperException, InterruptedException {
        String predecessorZnodeName = "";
        Stat predecessorStat = null;
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if(smallestChild.equals(currentZnodeName)) {
                System.out.println("We are the leader ");
                return;
            }else {
                System.out.println(" I am not the leader, " + smallestChild + " is the leader ");

                // Find predecessor node to watch for faliure
                int predecessorNode = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorNode);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName , this);
            }
        }
        System.out.println(" Watching znode " + predecessorZnodeName);
        System.out.println();
    }

    public void connectZookeeper() {
        try {
            this.zooKeeper  = new ZooKeeper(ZOOKEEPER_ADDRESS , SESSION_TIMEOUT , this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None -> {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to the zookeeper ");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from the zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
            }
            case NodeDeleted -> {
                try {
                    relcetLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
