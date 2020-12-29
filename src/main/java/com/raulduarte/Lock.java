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

public class Lock {
    
    private        String    root_lock  = Configuration.ROOT_LOCK;
    
    private static Semaphore mutex      = new Semaphore(1);
    private static boolean   update     = false;

    private        String    watchedNodePath;
    private static String    znode;

    private ZooKeeper zoo;

    ArrayList<String> childNodePaths;
    Stat              stat;


    public Lock(ZooKeeper zoo){

        this.zoo    = zoo;

        try {

            stat = zoo.exists(root_lock, false);
            if (stat == null) {
                zoo.create(root_lock, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            znode = zoo.create(root_lock + "/lock", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            //System.out.println("\nZNODE: " + znode + "\n");

            mutex.acquire();

            update = true;

            mutex.release();



        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public boolean getLock() throws InterruptedException, KeeperException {

        mutex.acquire();

        if (update) {

            mutex.release();

            childNodePaths = (ArrayList<String>) this.zoo.getChildren(root_lock, false);

            Collections.sort(childNodePaths);

            if ((root_lock + "/" + childNodePaths.get(0)).equals(znode)) {

                return true;

            } else {

                int index = 0;

                for (String s : childNodePaths) {
                    if ((root_lock + "/" + s).equals(znode)) {
                        break;
                    }
                    index++;
                }

                watchedNodePath = root_lock + "/" + childNodePaths.get(index - 1);

            }
        }

        watchNode(watchedNodePath, true);

        mutex.release();

        return false;
    }

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

    public void leave() throws InterruptedException, KeeperException {


        zoo.delete(znode, 0);

        //System.out.println("Lock Leave...");
    }

}
