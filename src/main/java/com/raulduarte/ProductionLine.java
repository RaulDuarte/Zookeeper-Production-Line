package com.raulduarte;

import com.raulduarte.config.Configuration;
import com.raulduarte.connection.*;
import com.raulduarte.thread.*;
import com.raulduarte.model.*;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ProductionLine {

    static String address            = Configuration.ADDRESS;
    static String root               = Configuration.ROOT;
    static String root_signal        = Configuration.ROOT_SIGNAL;
    static String manufactured_item  = Configuration.manufactured_item;
    static String root_lock          = Configuration.ROOT_LOCK;
    static String root_discard       = Configuration.ROOT_DISCARD;
    static String root_tag           = Configuration.ROOT_TAG;
    static String root_pack          = Configuration.ROOT_PACK;

    static Stat    stat;
    static Machine mach;
    static boolean control_thead = true;

    public static void main(String[] args) {

        /*****************************************/
        // Create the first znodes

        try {

            Connection conn = new ConnectionFactory(address);
            ZooKeeper zoo = conn.connect();

            /*****************************************/

            if (zoo != null) {

                /*****************************************/

                stat = zoo.exists(root, false);
                if (stat == null) {
                    zoo.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                stat = zoo.exists(root_signal, false);
                if (stat == null) {
                    zoo.create(root_signal, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                /*****************************************/

                Leader leader = new Leader(zoo);

                while (leader.isLeader() == false) {
                    control_thead = false;
                };



                /*****************************************/
                // Register Items

                stat = zoo.exists(manufactured_item, false);
                if (stat == null) {
                    zoo.create(manufactured_item, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    mach = new Machine("machine:", Configuration.PIECE_MACH);

                    for (long index = 0; index < Configuration.PIECE_MACH; index++) {
                        zoo.create(manufactured_item + "/item", new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT_SEQUENTIAL);
                    }
                }

                /*****************************************/

            }

            /*****************************************/
            // Initializes threads

            Sensor sensor = new Sensor(zoo);
            Discard discard = new Discard(zoo);
            Tag tag = new Tag(zoo);
            Pack pack = new Pack(zoo);

            /*****************************************/
            // Starts threads

            sensor.start();
            discard.start();
            tag.start();
            pack.start();

            /*****************************************/
            // Close connections

            sensor.join();
            discard.join();
            tag.join();
            pack.join();

            stat = zoo.exists(manufactured_item, false);
            if (stat != null) {
                zoo.delete(manufactured_item, 0);
            }

            System.out.println("Close Connection...");

            conn.close();

        } catch (IOException e) {

            e.printStackTrace();
        } catch (InterruptedException e) {

            e.printStackTrace();
        } catch (KeeperException e) {

            e.printStackTrace();
        }

    }
}
