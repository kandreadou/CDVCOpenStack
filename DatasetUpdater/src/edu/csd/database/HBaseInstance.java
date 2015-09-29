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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONValue;

import edu.csd.GlobalDescriptorVectorEntity;

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

	public void addToTable(String tupleString, String primaryKey, int id)
			throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		Put tuple = new Put(Bytes.toBytes(id));
		tuple.add(Bytes.toBytes("descriptorvector"),
				Bytes.toBytes("primarykey"), Bytes.toBytes(primaryKey));
		tuple.add(Bytes.toBytes("descriptorvector"), Bytes.toBytes("vector"),
				Bytes.toBytes(tupleString));
		table.put(tuple);

		table.flushCommits();
	}
	
	public void addPositionList(String positionList) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		Put tuple = new Put(Bytes.toBytes(1));
		tuple.add(Bytes.toBytes("descriptorvector"),
				Bytes.toBytes("positionlist"), Bytes.toBytes(positionList));
		
		table.put(tuple);

		table.flushCommits();
	}
	
	public String retrievePositionList() {
		String positionlist = null;
		byte[] prefix = Bytes.toBytes(1);
		Scan scan = new Scan(prefix);
		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		scan.setFilter(prefixFilter);
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				byte[] positionListBytes = result.getValue(
						Bytes.toBytes("descriptorvector"),
						Bytes.toBytes("positionlist"));
				return new String(positionListBytes);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return positionlist;
	}

	public String retrieveDescriptorVector(int id) {
		String descriptorVector = null;
		byte[] prefix = Bytes.toBytes(id);
		Scan scan = new Scan(prefix);
		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		scan.setFilter(prefixFilter);
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				byte[] descriptorVectorBytes = result.getValue(
						Bytes.toBytes("descriptorvector"),
						Bytes.toBytes("vector"));
				return new String(descriptorVectorBytes);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return descriptorVector;
	}

	public List<String> retrievePriorityIndex() {
		List<String> priorityIndexList = new ArrayList<>();
		byte[] prefix = Bytes.toBytes(1);
		Scan scan = new Scan(prefix);
		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		scan.setFilter(prefixFilter);
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				byte[] descriptorVectorBytes = result.getValue(
						Bytes.toBytes("priorityIndexs"),
						Bytes.toBytes("vector"));
				String priorityIndex = new String(descriptorVectorBytes);
				priorityIndexList.add(priorityIndex);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return priorityIndexList;
	}
	
	public List<GlobalDescriptorVectorEntity> retrieveCandidateDescriptorVectors(String primaryKey) {
		List<GlobalDescriptorVectorEntity> candidateList = new ArrayList<>();
		byte[] prefix = Bytes.toBytes(1);
		byte[] descriptorVector = Bytes.toBytes("descriptorvector");
		byte[] primarykey = Bytes.toBytes("primarykey");
		byte[] vectorBytes = Bytes.toBytes("vector");
		RegexStringComparator primaryKeyFilter = new RegexStringComparator(primaryKey);
		SingleColumnValueFilter filter = new SingleColumnValueFilter(descriptorVector, primarykey, CompareOp.EQUAL, primaryKeyFilter);
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				String id = new String(result.getRow());
				byte[] descriptorVectorBytes = result.getValue(
						descriptorVector, vectorBytes
						);
				String candidateDescriptorVector = new String(descriptorVectorBytes);
				
				byte[] pkbytes = result.getValue(descriptorVector, primarykey);
				String primarykeyValue = new String(pkbytes);
				GlobalDescriptorVectorEntity candidateEntity = new GlobalDescriptorVectorEntity(primarykeyValue, Integer.parseInt(id), candidateDescriptorVector);
				candidateList.add(candidateEntity);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		return candidateList;
	}
}
