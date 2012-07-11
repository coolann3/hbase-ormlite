package com.wlu.orm.hbase.dao;

import java.lang.reflect.InvocationTargetException;

import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.DataMapper;
import com.wlu.orm.hbase.schema.value.Value;

public class DaoImpl<T> implements Dao<T> {

	Class<T> dataClass;
	private HBaseConnection hbaseConnection;
	// set constant schemas
	@SuppressWarnings("unused")
	private final DataMapper dm = new DataMapper<T>(dataClass);

	public DaoImpl(Class<T> dataClass, HBaseConnection connection)
			throws HBaseOrmException {
		this.dataClass = dataClass;
		hbaseConnection = connection;
	}

	@Override
	public void Create() {
		if (hbaseConnection.TableExists(DataMapper.tablename)) {
			hbaseConnection.DeleteTable(DataMapper.tablename);
		}
		hbaseConnection.CreateTable(DataMapper.TableCreateDescriptor());
	}

	@Override
	public void CreateIfNotExist() {
		if (hbaseConnection.TableExists(DataMapper.tablename)) {
			return;
		}
		hbaseConnection.CreateTable(DataMapper.TableCreateDescriptor());

	}

	@Override
	public void Insert(T data) {
		DataMapper<T> dataMapper = null;
		try {
			dataMapper = new DataMapper<T>(data);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (HBaseOrmException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		dataMapper.Insert(hbaseConnection);
	}

	@Override
	public void DeleteById(Value rowkey) {
		// TODO
		
	}

	@Override
	public void DeleteById(T data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void Delete(T data, String familyFieldName, String qualifierFieldName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void Delete(T data, String familyFieldName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void Delete(T data) {
		// TODO Auto-generated method stub
		
	}

}
