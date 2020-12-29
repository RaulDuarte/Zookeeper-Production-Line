package com.raulduarte.thread;

import com.raulduarte.config.Configuration;
//import com.raulduarte.connection.*;
import com.raulduarte.Barrier;
import com.raulduarte.Lock;

//import java.io.IOException;
import java.util.ArrayList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Pack extends Thread {

	//private String  address      = Configuration.ADDRESS;
	private long    velocity  	 = Configuration.VELOCITY;
	private int     pkg_size     = Configuration.PACKAGE_SIZE;

	private String  barrier_pack = Configuration.BARRIER_PACK;
	private String  signal_pack  = Configuration.SIGNAL_PACK;
	private String  root_signal  = Configuration.ROOT_SIGNAL;
	private String  root_pack    = Configuration.ROOT_PACK;

	private Stat      stat;
	private ZooKeeper zoo;

	Lock lock;


	public Pack(ZooKeeper zoo) {
		this.zoo = zoo;
	}

	public void config(ZooKeeper zoo) throws KeeperException, InterruptedException {

		stat = zoo.exists(signal_pack, false);
		if (stat == null) {
			zoo.create(signal_pack, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}

	}

	String consume(ZooKeeper zoo) throws KeeperException, InterruptedException {

		while (true) {

			boolean flag = false;

			ArrayList<String> list = (ArrayList<String>) zoo.getChildren(root_pack, false);
			ArrayList<String> signal = (ArrayList<String>) zoo.getChildren(root_signal, false);

			if (list.isEmpty()) {

				for (String s : signal) {
					if (s.equals("tag")) {
						flag = true;
					}
				}

				if (flag) {

					System.out.println("Pack awaiting tag item");

					Thread.sleep(velocity);
				} else {
					zoo.delete(signal_pack, 0);
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

				System.out.println("Pack consulted item " + root_pack + "/item" + minimum_value);

				byte[] data = zoo.getData(root_pack + "/item" + minimum_value, false, null);

				zoo.delete(root_pack + "/item" + minimum_value, 0);

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

			config(zoo);

			ArrayList<String> synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);

			while(synchronize.size() < 4){
				synchronize = (ArrayList<String>) zoo.getChildren(root_signal, true);
			}

			System.out.println("Pack.class STARTED...");

			Barrier barrier = new Barrier(zoo, this.barrier_pack, pkg_size);

			while (true) {

				String data = consume(zoo);

				if(data == null){

					ArrayList<String> list = (ArrayList<String>) zoo.getChildren(this.barrier_pack, true);

					if(list.size() > 0){
		
						System.out.println("\n===========================================\nPackage with " + list.size() 
							+ " items waiting to be created\n===========================================");
		
						for(String node : list){
							byte[] data_node = zoo.getData(this.barrier_pack + "/" + node, false, null);	
		
							System.out.println("Item --> " + new String(data_node)); 
						}
		
						System.out.println("===========================================\n");

						barrier.leave(zoo);
					}

					//conn.close();
					break;
				}

				System.out.println("Pack produced an item " + data);

				System.out.println("Barrier Pack");

				if(barrier.insert(zoo, data)){

					lock = new Lock(zoo);

					while(!lock.getLock());

					System.out.println("Lock Pack");

					ArrayList<String> list = (ArrayList<String>) zoo.getChildren(this.barrier_pack, true);


					System.out.println("\n===========================================\nPackage with " + pkg_size 
						+ " items created\n===========================================");

					for(String node : list){
						byte[] data_node = zoo.getData(this.barrier_pack + "/" + node, true, null);	

						System.out.println("Item --> " + new String(data_node)); 
					}

					System.out.println("===========================================\n");

					barrier.leave(zoo);

					lock.leave();

					//System.out.println("TESTE 2");

				}

			}

			System.out.println("Pack FINISH...");
			
		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}
}