package com.raulduarte.thread;

//import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

//import com.raulduarte.connection.*;
import com.raulduarte.config.Configuration;

//import com.raulduarte.connection.Connection;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Discard extends Thread {

	//private String address  		= Configuration.ADDRESS;
	private long   velocity 		= Configuration.VELOCITY;
	private int    failure_rate 	= Configuration.FAILURE_RATE;
	private String root_tag 		= Configuration.ROOT_TAG;
	private String root_discard 	= Configuration.ROOT_DISCARD;
	private String signal_discard 	= Configuration.SIGNAL_DISCARD;
	private String root_sensor      = Configuration.ROOT_SENSOR;
	private String root_signal 		= Configuration.ROOT_SIGNAL;
//	private String root 			= Configuration.ROOT;

	private String    data;
	private Stat      stat;
	private ZooKeeper zoo;

	Random 	generator   = new Random(0);

	public Discard(ZooKeeper zoo) {
		this.zoo = zoo;
	}

	public void config(ZooKeeper zoo) throws KeeperException, InterruptedException {

		stat = zoo.exists(root_discard, false);
		if (stat == null) {
			zoo.create(root_discard, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		stat = zoo.exists(signal_discard, false);
		if (stat == null) {
			zoo.create(signal_discard, "discard".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}

	}

	public void create_queue(ZooKeeper zoo, String root_tag) throws KeeperException, InterruptedException {

		this.root_tag = root_tag;

		if (zoo != null) {

			stat = zoo.exists(root_tag, false);
			if (stat == null) {
				zoo.create(root_tag, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}

	boolean produce(ZooKeeper zoo, byte[] data) throws KeeperException, InterruptedException{

		zoo.create(root_tag+"/item", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		return true;
	}

	String consume(ZooKeeper zoo) throws KeeperException, InterruptedException{

		while (true) {

				boolean flag = false;	

				ArrayList<String> list 			 = (ArrayList<String>) zoo.getChildren(root_sensor, true);
				ArrayList<String> signal = (ArrayList<String>) zoo.getChildren(root_signal, true);

				if (list.isEmpty()) {
					
					for(String s : signal){
						if(s.equals("sensor")){
							flag = true;
						}
					}

					if(flag){

						System.out.println("Discard awaiting sensor item");

						Thread.sleep(velocity);
					}else{
						zoo.delete(signal_discard, 0);
						return null;
					}

				} else {

					Integer min = new Integer(list.get(0).substring(4));

					for(String s : list){

						Integer tempValue = new Integer(s.substring(4));

						if(tempValue < min){
							min = tempValue;
						} 
					}

					String minimum_value = String.format("%010d", min);

					System.out.println("Discard consumed item " + "/production_line/sensor/item" +  minimum_value);

					byte[] data = zoo.getData(root_sensor + "/item" + minimum_value, false, null);

					zoo.delete(root_sensor + "/item" + minimum_value, 0);

					return new String(data);
				}
		}
	}

	boolean discard_item(ZooKeeper zoo, byte[] data) throws KeeperException, InterruptedException{

		zoo.create(root_discard + "/item", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		return true;
	}

	public void run() {

		try {

			/*****************************************/
			// Create connection
			/*
			Connection conn = new ConnectionFactory(address);
			ZooKeeper zoo = conn.connect();
			*/
			/*****************************************/
			
			create_queue(zoo, root_tag);
			config(zoo);

			ArrayList<String> synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);

			while(synchronize.size() < 4){
				synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);
			}

			System.out.println("Discard.class STARTED...");


			while(true){

				data = consume(zoo);

				if(data == null){

					ArrayList<String> list = (ArrayList<String>) zoo.getChildren(this.root_discard, true);

					if(list.size() > 0){

		
						System.out.println("\n===========================================\n" + list.size()
							+ " defective items have been discarded\n===========================================");
		
						for(String node : list){
							byte[] data_node = zoo.getData(this.root_discard+ "/" + node, false, null);	
		
							System.out.println("Item --> " + new String(data_node)); 
							
							zoo.delete(this.root_discard+ "/" + node, 0);
						}
		
						System.out.println("===========================================\n");

					}

					//conn.close();
					break;
				}

				if(generator.nextInt(101) >= failure_rate){

					produce(zoo, (data + ":discard").getBytes());	

					System.out.println("Discard produced an item " + data);

				}else{

					discard_item(zoo, (data + ":discard").getBytes());

					System.out.println("Defective item has been discarded " + data);

				}
			}

			System.out.println("Discard FINISH...");

		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
}