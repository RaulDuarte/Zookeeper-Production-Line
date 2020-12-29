package com.raulduarte;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Semaphore;

import com.raulduarte.config.Configuration;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Leader {


    private ZooKeeper zoo;
    private String znode;

    private String watchedNodePath;
    private ArrayList<String> childNodePaths;

    private static Semaphore mutex        = new Semaphore(1);
    private static boolean   update       = false;
    private        String    root_leader  = Configuration.ROOT_LEADER;

    Stat stat;
    

    public Leader(ZooKeeper zoo) {

        this.zoo = zoo;

        try {

            stat = zoo.exists(root_leader, false);
            if (stat == null) {
                zoo.create(root_leader, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            znode = this.zoo.create(root_leader + "/p_", new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);

            mutex.acquire();

            update = true;

            mutex.release();

            System.out.println("\nZNODE: " + znode + "\n");

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isLeader() throws InterruptedException, KeeperException {

        mutex.acquire();

        if (update) {

            mutex.release();

            childNodePaths = (ArrayList<String>) this.zoo.getChildren(root_leader, false);

            Collections.sort(childNodePaths);

            if ((root_leader + "/" + childNodePaths.get(0)).equals(znode)) {

                System.out.println("New Leader Defined...");

                return true;

            } else {

                int index = 0;

                for (String s : childNodePaths) {
                    if ((root_leader + "/" + s).equals(znode)) {
                        break;
                    }
                    index++;
                }

                watchedNodePath = root_leader + "/" + childNodePaths.get(index - 1);

                watchNode(watchedNodePath, true);
            }
        }

        mutex.release();

        return false;
    }

    /*
     * public boolean watchNode(String node, boolean watch) {
     * 
     * boolean watched = false;
     * 
     * try { final Stat nodeStat = zoo.exists(node, watch);
     * 
     * if(nodeStat != null) { watched = true; }
     * 
     * } catch (KeeperException | InterruptedException e) { throw new
     * IllegalStateException(e); }
     * 
     * return watched; }
     */

    public void watchNode(String node, boolean watch) throws KeeperException, InterruptedException {

        zoo.exists(node, watcher);

        mutex.acquire();

        update = false;

        mutex.release();
    }

    Watcher watcher = new Watcher() {

        public void process(WatchedEvent e) {

            try {

                mutex.acquire();

                update = true;

                mutex.release();

            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            
        }
    };

}
