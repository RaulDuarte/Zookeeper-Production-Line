package com.raulduarte.connection;

import java.io.IOException;
import org.apache.zookeeper.ZooKeeper;

public interface Connection {
    
    public ZooKeeper connect() throws IOException,InterruptedException;
    public void close() throws InterruptedException;

}
