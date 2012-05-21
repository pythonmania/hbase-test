package com.lsworks.hadoop.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class HBasePerformanceTest {
	final String tableName = "sample";
	final byte[] keyFamilyName = "userid".getBytes();
	final byte[] keyQualifier = "userid".getBytes();
	final byte[] valueFamilyName = "name".getBytes();
	final byte[] valueQualifier = "name".getBytes();

	public static void main(String[] args) throws IOException, KeeperException,
			InterruptedException {
		HBasePerformanceTest hbpt = new HBasePerformanceTest();
		hbpt.runTest();
	}

	private void runTest() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.10.24");
		conf.setInt("hbase.zookeeper.property.clientPort", 2181);

		deleteUsersTable(conf);

		createUsersTable(conf);

		for (int i = 1; i < Integer.MAX_VALUE; i++) {
			System.out.println("********** phase " + i + " **********");
			
			Map<String, String> sample = generateSampleMap(30);

			put(conf, sample);

			get(conf, sample);
		}
	}

	private Map<String, String> generateSampleMap(int count) {
		Map<String, String> sample = new HashMap<String, String>();
		for (int i = 0; i < count; i++) {
			String key = RandomStringUtils.randomAlphanumeric(10);
			String value = RandomStringUtils.randomAlphanumeric(50);
			sample.put(key, value);
		}

		return sample;
	}

	private void deleteUsersTable(Configuration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	private void createUsersTable(Configuration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(new HColumnDescriptor(keyFamilyName));
		tableDesc.addFamily(new HColumnDescriptor(valueFamilyName));
		admin.createTable(tableDesc);
	}

	private void put(Configuration conf, Map<String, String> sample)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		for (String key : sample.keySet()) {
			Put put = new Put(key.getBytes());
			put.add(keyFamilyName, keyQualifier, key.getBytes());
			put.add(valueFamilyName, valueQualifier, sample.get(key).getBytes());
			table.put(put);
		}
		table.close();
	}

	private void get(Configuration conf, Map<String, String> sample)
			throws IOException {
		HTable table = new HTable(conf, tableName);
		for (String key : sample.keySet()) {
			Get get = new Get(key.getBytes());
			Result r = table.get(get);

			String value = Bytes.toString(r.getValue(valueFamilyName,
					valueQualifier));
			if (sample.get(key).equals(value)) {
				System.out.format("match. key:%s, value:%s\n", key, value);
			} else {
				System.err.format("mismatch. key:%s, expected:%s, result:%s\n",
						key, sample.get(key), value);
			}
		}
		table.close();
	}
}
