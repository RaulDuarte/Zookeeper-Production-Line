package com.raulduarte.thread;

//import java.io.IOException;
import java.util.ArrayList;

import com.raulduarte.config.Configuration;
//import com.raulduarte.connection.*;
import com.raulduarte.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Tag extends Thread {

	//private String address     = Configuration.ADDRESS;
	private long   velocity    = Configuration.VELOCITY;
	private String root_pack   = Configuration.ROOT_PACK;
	private String signal_tag  = Configuration.SIGNAL_TAG;
	private String root_tag    = Configuration.ROOT_TAG;
	private String root_signal = Configuration.ROOT_SIGNAL;
	//private String root		   = Configuration.ROOT;

	private Stat      stat;
	private ZooKeeper zoo;

	Lock lock;

	
	public Tag(ZooKeeper zoo) {
		this.zoo = zoo;
	}

	public void config(ZooKeeper zoo) throws KeeperException, InterruptedException {

		stat = zoo.exists(signal_tag, false);
		if (stat == null) {
			zoo.create(signal_tag, "tag".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
	}

	public void create_queue(ZooKeeper zoo, String root_pack) throws KeeperException, InterruptedException {

		this.root_pack = root_pack;

		if (zoo != null) {
			
			stat = zoo.exists(root_pack, false);
			if (stat == null) {
				zoo.create(root_pack, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}

	boolean produce(ZooKeeper zoo, byte[] data) throws KeeperException, InterruptedException {

		//Lock lock = new Lock(zoo);
		//while(!lock.getLock());

		zoo.create(root_pack + "/item", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

		//lock.leave();

		return true;
	}

	String consume(ZooKeeper zoo) throws KeeperException, InterruptedException {

		while (true) {
			
			boolean flag = false;	

			ArrayList<String> list = (ArrayList<String>) zoo.getChildren(root_tag, false);
			ArrayList<String> signal = (ArrayList<String>) zoo.getChildren(root_signal, false);

			if (list.isEmpty()) {

				for(String s : signal){
					if(s.equals("discard")){
						flag = true;
					}
				}

				if(flag){

					System.out.println("Tag awaiting discard item");

					Thread.sleep(velocity);
				}else{
					zoo.delete(signal_tag, 0);
					return null;
				}

			} else {

				Integer min = new Integer(list.get(0).substring(4));

				for (String s : list) {

					Integer tempValue = new Integer(s.substring(4));

					if (tempValue < min) {
						min = tempValue;
					}
				}

				String minimum_value = String.format("%010d", min);

				System.out.println("Tag consumed item " + "/production_line/tag/item" + minimum_value);

				byte[] data = zoo.getData(root_tag + "/item" + minimum_value, false, null);

				zoo.delete(root_tag + "/item" + minimum_value, 0);

				return new String(data);
			}
		}
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

			create_queue(zoo, root_pack);
			config(zoo);

			ArrayList<String> synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);

			while(synchronize.size() < 4){
				synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);
			}

			System.out.println("Tag.class STARTED...");

			while (true) {

				String data = consume(zoo);

				if(data == null){
					//conn.close();
					break;
				}

				if (produce(zoo, (data + ":tag").getBytes())) {

					System.out.println("Tag produced an item " + data);
				}
			}

			System.out.println("Tag FINISH...");

		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}
}