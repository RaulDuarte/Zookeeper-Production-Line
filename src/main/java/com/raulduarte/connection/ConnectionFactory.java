package com.raulduarte.connection;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class ConnectionFactory implements Connection{
    
    private String address;
    private ZooKeeper conn;

    Stat stat;

    final CountDownLatch connectedSignal = new CountDownLatch(1);


    public ConnectionFactory(String address){
        this.address = address;
    }   

    public ZooKeeper connect() throws IOException,InterruptedException {
	
        conn = new ZooKeeper(address,2000,new Watcher() {
		
         public void process(WatchedEvent we) {

            if (we.getState() == KeeperState.SyncConnected) {

               //System.out.println("PING");
               
               connectedSignal.countDown();
            }
         }
      });
		
      connectedSignal.await();

      return conn;
   }

   public void close() throws InterruptedException {
      conn.close();
   }
}



