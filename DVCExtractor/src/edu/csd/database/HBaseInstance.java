package edu.csd.database;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.filefilter.PrefixFileFilter;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseInstance {
	private Configuration config;
	private HTable table;

	public HBaseInstance(String tableName) {
		config = HBaseConfiguration.create();
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			tableDescriptor
					.addFamily(new HColumnDescriptor("descriptorvector"));
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

	public void addToTable(List<String> tuples, int counter)
			throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		for (int i = 0; i < tuples.size(); i++) {
			Put tuple = new Put(Bytes.toBytes(counter++));
			tuple.add(Bytes.toBytes("descriptorvector"),
					Bytes.toBytes("vector"), Bytes.toBytes(tuples.get(i)));
			table.put(tuple);
		}
		table.flushCommits();
	}

	public List<String> retrieveDescriptorVector(int N) {
		List<String> descriptorVectors = new ArrayList<>();
		for(int i = 1; i <= N; i++) {
			byte[] prefix = Bytes.toBytes(i);
			Scan scan = new Scan(prefix);
			PrefixFilter prefixFilter = new PrefixFilter(prefix);
			scan.setFilter(prefixFilter);
			try {
				ResultScanner results = table.getScanner(scan);
				for(Result result : results) {
					byte[] descriptorVectorBytes = result.getValue(Bytes.toBytes("descriptorvector"), Bytes.toBytes("vector"));
					String descriptorVector = new String(descriptorVectorBytes);
					descriptorVectors.add(descriptorVector);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return descriptorVectors;
	}
}
