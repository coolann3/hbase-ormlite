package com.wlu.orm.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

public class HBaseConnection {

	private HTablePool pool;
	private HBaseAdmin admin;

	public HBaseConnection(String Zk, String Port, int PoolSize) {
		Configuration cfg = new Configuration();
		cfg.set("hbase.zookeeper.quorum", Zk);
		cfg.set("hbase.zookeeper.property.clientPort", Port);
		pool = new HTablePool(cfg, PoolSize);
		try {
			admin = new HBaseAdmin(cfg);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	public void Insert(byte[] tablename, Put put) {
		HTable htable = (HTable) pool.getTable(tablename);
		try {
			htable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean TableExists(final String tableName) {
		try {
			return admin.tableExists(tableName);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public void DeleteTable(final String tableName) {
		try {
			admin.deleteTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void CreateTable(HTableDescriptor td) {
		try {
			admin.createTable(td);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
