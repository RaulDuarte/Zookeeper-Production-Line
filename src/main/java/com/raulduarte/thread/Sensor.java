package com.raulduarte.thread;

//import java.io.IOException;
import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;

import com.raulduarte.config.Configuration;
//import com.raulduarte.connection.*;
//import com.raulduarte.model.Machine;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


public class Sensor extends Thread {

	//private String address  	 = Configuration.ADDRESS;
	private long   velocity 	 = Configuration.VELOCITY;
	private String root_sensor   = Configuration.ROOT_SENSOR;
	private String signal_sensor = Configuration.SIGNAL_SENSOR;
	//private String root          = Configuration.ROOT;
	private String root_signal   = Configuration.ROOT_SIGNAL;
	private String manufactured_item    = Configuration.manufactured_item;

	private Stat      stat;
	private ZooKeeper zoo;
	private ArrayList<String> items, synchronize;
	

	public Sensor(ZooKeeper zoo) {
		this.zoo = zoo;
	}

	public void config(ZooKeeper zoo) throws KeeperException, InterruptedException {

		stat = zoo.exists(signal_sensor, false);
		if (stat == null) {
			zoo.create(signal_sensor, "sensor".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}

	}

	public void create_queue(ZooKeeper zoo, String root_sensor) throws KeeperException, InterruptedException {

		this.root_sensor = root_sensor;

		if (zoo != null) {

			stat = zoo.exists(root_sensor, false);
			if (stat == null) {
				zoo.create(root_sensor, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}

	}

	boolean produce(ZooKeeper zoo, byte[] data) throws KeeperException, InterruptedException{

		zoo.create(root_sensor+"/item", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		return true;
	}

	public void run() {

		String 	data 				= "";

		try {

			/*****************************************/
			// Create connection
			/*
			Connection conn = new ConnectionFactory(address);
			ZooKeeper zoo = conn.connect();
			*/
			/*****************************************/

			create_queue(zoo, root_sensor);
			config(zoo);

			synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);

			while(synchronize.size() < 4){
				synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);
			}


			System.out.println("Sensor.class STARTED...");


			items = (ArrayList<String>) zoo.getChildren(manufactured_item, true);

			while (items.size() > 0) {

					data = "machine:sensor";

					if( produce(zoo, data.getBytes()) ){
					

						System.out.println("Sensor produced an item " + data);


						zoo.delete(manufactured_item + "/" + items.get(0), 0);
					}

					items = (ArrayList<String>) zoo.getChildren(manufactured_item, true);
					Thread.sleep(velocity);
				}

			zoo.delete(signal_sensor, 0);

			while(true){

				boolean flag = false;

				ArrayList<String> signal = (ArrayList<String>) zoo.getChildren(root_signal, true);

				for(String s : signal){
					if(s.equals("discard")){
						flag = true;
					}
				}

				if(!flag){
					//conn.close();
					break;
				}
			}

			System.out.println("Sensor FINISH...");


		} catch (KeeperException e) {
			e.printStackTrace();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}