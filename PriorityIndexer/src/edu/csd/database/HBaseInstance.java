package edu.csd.database;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

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

import edu.csd.PriorityIndexValue;

public class HBaseInstance {
	private Configuration config;
	private HTable table;

	public HBaseInstance(String tableName) {
		config = HBaseConfiguration.create();
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			tableDescriptor.addFamily(new HColumnDescriptor("priorityindex"));
			admin.createTable(tableDescriptor);
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


	public void addToTable(List<PriorityIndexValue> priorityIndex, int counter) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		String priorityIndexString = "";
		for(int i = 0; i < priorityIndex.size() ;i++) {
			priorityIndexString += priorityIndex.get(i).getDimension() + ",";
		}
		priorityIndexString = priorityIndexString.substring(0, priorityIndexString.length() - 1);
		Put tuple = new Put(Bytes.toBytes(counter));
		tuple.add(Bytes.toBytes("priorityIndex"), Bytes.toBytes("vector"), Bytes.toBytes(priorityIndexString));
		table.put(tuple);
		table.flushCommits();
	}
}
