package com.raulduarte;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Barrier {

    private String root;
    private int    size;


    public Barrier(ZooKeeper zoo, String root, int size) {

        this.root    = root;
        this.size = size;

        // Create barrier node
        if (zoo != null) {
            try {
                Stat s = zoo.exists(root, false);
                if (s == null) {
                    zoo.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {

                System.out.println("Keeper exception when instantiating queue: "+ e.toString());
            } catch (InterruptedException e) {

                System.out.println("Interrupted exception");
            }
        }
    }


    public boolean insert(ZooKeeper zoo, String data) throws KeeperException, InterruptedException{
        
        zoo.create(root + "/item", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        while (true) {

            List<String> list = zoo.getChildren(root, true);

            if (list.size() < size) {
                return false;

            } else {

                return true;
            }
            
        }
    }

    public boolean leave(ZooKeeper zoo) throws KeeperException, InterruptedException{

        ArrayList<String> list = (ArrayList<String>) zoo.getChildren(root, true);

        for(String node : list){
            zoo.delete(root + "/" + node, 0);
        }

        return true;
        
    }
}
