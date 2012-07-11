package com.wlu.orm.hbase.dao;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.DataMapper;
import com.wlu.orm.hbase.schema.DataMapperFacory;
import com.wlu.orm.hbase.schema.value.Value;
import com.wlu.orm.hbase.schema.value.ValueFactory;
import com.wlu.orm.hbase.util.util;

public class DaoImpl<T> implements Dao<T> {

	Log LOG = LogFactory.getLog(DaoImpl.class);
	Class<T> dataClass;
	private HBaseConnection hbaseConnection;
	// set constant schemas
	private DataMapperFacory<T> dataMapperFactory = null;

	public DaoImpl(Class<T> dataClass, HBaseConnection connection)
			throws HBaseOrmException {
		this.dataClass = dataClass;
		hbaseConnection = connection;
		dataMapperFactory = new DataMapperFacory<T>(dataClass);
	}

	@Override
	public void Create() {
		if (hbaseConnection.TableExists(dataMapperFactory.tablename)) {
			hbaseConnection.DeleteTable(dataMapperFactory.tablename);
		}
		hbaseConnection.CreateTable(dataMapperFactory.TableCreateDescriptor());
	}

	@Override
	public void CreateIfNotExist() {
		if (hbaseConnection.TableExists(dataMapperFactory.tablename)) {
			return;
		}
		hbaseConnection.CreateTable(dataMapperFactory.TableCreateDescriptor());

	}

	@Override
	public void Insert(T data) {
		// need to check the type
		if (!data.getClass().equals(dataClass)) {
			LOG.error("Class type of data is not the same as that of the Dao, should be "
					+ dataClass);
			return;
		}
		DataMapper<T> dataMapper = null;
		try {
			dataMapper = dataMapperFactory.Create(data);
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
		org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
				rowkey.toBytes());
		hbaseConnection.Delete(Bytes.toBytes(dataMapperFactory.tablename),
				delete);

	}

	@Override
	public void DeleteById(T data) {
		try {
			Value rowkey = ValueFactory.Create(util.GetFromField(data,
					dataMapperFactory.rowkeyField));
			DeleteById(rowkey);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

	}

	@Override
	/**
	 * The qualifier is pretty complicated
	 */
	public void Delete(T data, String familyFieldName, String qualifierFieldName) {
		if (qualifierFieldName == null) {
			Delete(data, familyFieldName);
			return;
		}
		Value rowkey;
		try {

			rowkey = ValueFactory.Create(util.GetFromField(data,
					dataMapperFactory.rowkeyField));
			org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
					rowkey.toBytes());
			// get family name
			Field familyNameField = data.getClass().getDeclaredField(
					familyFieldName);
			byte[] familyname = GetFamilyByFieldName(familyNameField,
					familyFieldName);
			// get qualifier name
			byte[] qualifiername = GetQualiferByFamilyOrSublevelFieldName(
					familyNameField, qualifierFieldName);

			delete.deleteColumn(familyname, qualifiername);
			hbaseConnection.Delete(Bytes.toBytes(dataMapperFactory.tablename),
					delete);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (HBaseOrmException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void Delete(T data, String familyFieldName) {
		if (familyFieldName == null) {
			Delete(data);
			return;
		}
		Value rowkey;
		try {
			rowkey = ValueFactory.Create(util.GetFromField(data,
					dataMapperFactory.rowkeyField));
			org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
					rowkey.toBytes());
			// get family name
			Field familyNameField = data.getClass().getDeclaredField(
					familyFieldName);
			byte[] familyname = GetFamilyByFieldName(familyNameField,
					familyFieldName);
			delete.deleteFamily(familyname);
			hbaseConnection.Delete(Bytes.toBytes(dataMapperFactory.tablename),
					delete);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}

	}

	private byte[] GetFamilyByFieldName(Field familyNameField,
			String familyFieldName) throws SecurityException,
			NoSuchFieldException {
		byte[] familyname = dataMapperFactory.fixedSchema.get(familyNameField)
				.getFamily();
		return familyname;
	}

	private byte[] GetQualiferByFamilyOrSublevelFieldName(
			Field familyNameField, String qualifierFieldName)
			throws HBaseOrmException {
		// if qualifier name is set with family name
		byte[] qualifiername = dataMapperFactory.fixedSchema.get(
				familyNameField).getQualifier();
		// qualifier is not directly set or set with a wrong value (for List
		// type, it is wrong)
		if (qualifiername == null
				|| Bytes.compareTo(qualifiername,
						Bytes.toBytes(qualifierFieldName)) != 0) {
			qualifiername = null;
		}
		if (qualifiername == null) {
			Map<String, byte[]> subFieldToQualifier = dataMapperFactory.fixedSchema
					.get(familyNameField).getSubFieldToQualifier();
			if (subFieldToQualifier == null) {
				qualifiername = null;
			} else if (subFieldToQualifier.get(qualifierFieldName) != null) {
				qualifiername = subFieldToQualifier.get(qualifierFieldName);
			} else {
				throw new HBaseOrmException("The field '" + qualifierFieldName
						+ "' of sub level family class '"
						+ familyNameField.getName() + "' is not set as qualier");
			}
			// else qualifier is set with name of the field's name
			if (qualifiername == null) {
				qualifiername = Bytes.toBytes(qualifierFieldName);
			}
		}
		return qualifiername;
	}

	@Override
	public void Delete(T data) {
		DeleteById(data);

	}

}
