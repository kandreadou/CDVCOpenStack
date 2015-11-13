package edu.csd.database;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseInstance {
	private Configuration config;
	private HTable table;

	public HBaseInstance(String tableName) {
		config = HBaseConfiguration.create();
		config.setInt("timeout", 120000); 
		config.set("hbase.master", "localhost"); 
		
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			tableDescriptor
					.addFamily(new HColumnDescriptor("descriptorvector"));
			if(!admin.isTableAvailable(tableName)){
				admin.createTable(tableDescriptor);
			}
			table = new HTable(config, tableName);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addToTable(Map<String, String> tuples, int counter) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		for (String key: tuples.keySet()) {
			Put tuple = new Put(Bytes.toBytes(counter));
			tuple.add(Bytes.toBytes("descriptorvector"), Bytes.toBytes("id"), Bytes.toBytes(key));
			tuple.add(Bytes.toBytes("descriptorvector"), Bytes.toBytes("vector"), Bytes.toBytes(tuples.get(key)));
			table.put(tuple);
			counter++;
		}
		table.flushCommits();
	}

}
