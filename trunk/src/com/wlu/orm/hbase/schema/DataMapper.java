package com.wlu.orm.hbase.schema;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.wlu.orm.hbase.annotation.DatabaseField;
import com.wlu.orm.hbase.annotation.DatabaseTable;
import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.value.Value;
import com.wlu.orm.hbase.schema.value.ValueFactory;
import com.wlu.orm.hbase.util.util;

/**
 * Each
 * 
 * @author Administrator
 * 
 * @param <T>
 */
public class DataMapper<T> {
	Log LOG = LogFactory.getLog(DataMapper.class);
	// fixed schema for the generic type T, copy from the factory
	public String tablename;
	public Map<Field, FamilyQualifierSchema> fixedSchema;
	public Map<Field, FieldDataType> fieldDataType;
	public Field rowkeyField;
	public Class<?> dataClass;

	// private data for individual instance
	private Map<Field, FamilytoQualifersAndValues> datafieldsToFamilyQualifierValue;
	// private data for rowkey
	private Value rowkey;

	/**
	 * Construct with fixed members as parameters
	 * 
	 * @param tablename
	 * @param fixedSchema
	 * @param rowkeyField
	 * @param dataClass
	 */
	public DataMapper(String tablename,
			Map<Field, FamilyQualifierSchema> fixedSchema,
			Map<Field, FieldDataType> fieldDataType, Field rowkeyField,
			Class<?> dataClass) {
		this.tablename = tablename;
		this.fieldDataType = fieldDataType;
		this.fixedSchema = fixedSchema;
		this.rowkeyField = rowkeyField;
		this.dataClass = dataClass;
	}

	// insert the instance to HBase
	public void Insert(HBaseConnection connection) {
		Put put = new Put(rowkey.toBytes());
		// add each field's data to the 'put'
		for (Field field : datafieldsToFamilyQualifierValue.keySet()) {
			datafieldsToFamilyQualifierValue.get(field).AddToPut(put);
		}

		connection.Insert(Bytes.toBytes(tablename), put);
	}

	/**
	 * Copy from the fixed schema. All members used in the method are fixed
	 * according to the <code>dataClass</code>
	 * 
	 * @throws HBaseOrmException
	 */
	public void CopyToDataFieldSchemaFromFixedSchema() throws HBaseOrmException {
		datafieldsToFamilyQualifierValue = new HashMap<Field, FamilytoQualifersAndValues>();
		for (Field field : fixedSchema.keySet()) {
			FamilyQualifierSchema fqv = fixedSchema.get(field);
			if (fqv.getFamily() == null) {
				throw new HBaseOrmException("Family should not be null!");
			}
			// if(fqv.getQualifier()== null){
			FamilytoQualifersAndValues f2qvs = new FamilytoQualifersAndValues();
			f2qvs.setFamily(fqv.getFamily());
			datafieldsToFamilyQualifierValue.put(field, f2qvs);
			// }

		}
	}

	public Map<Field, FamilytoQualifersAndValues> getDatafieldsToFamilyQualifierValue() {
		return datafieldsToFamilyQualifierValue;
	}

	public void setDatafieldsToFamilyQualifierValue(
			Map<Field, FamilytoQualifersAndValues> datafieldsToFamilyQualifierValue) {
		this.datafieldsToFamilyQualifierValue = datafieldsToFamilyQualifierValue;
	}

	/**
	 * Create a concret DataMapper instance by filling rowkey, family:qualifier
	 * etc
	 * 
	 * @param instance
	 * @throws IllegalArgumentException
	 * @throws HBaseOrmException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public void CopyToDataFieldsFromInstance(T instance)
			throws IllegalArgumentException, HBaseOrmException,
			IllegalAccessException, InvocationTargetException {
		for (Field field : instance.getClass().getDeclaredFields()) {
			// if is rowkey
			if (rowkeyField.equals(field)) {
				rowkey = ValueFactory
						.Create(util.GetFromField(instance, field));
				continue;
			}
			FamilyQualifierSchema fq = fixedSchema.get(field);
			FieldDataType fdt = fieldDataType.get(field);
			// field not included for HBase
			if (fq == null) {
				continue;
			}

			// Primitive, family and qualifier name are both specified
			if (fq.getQualifier() != null) {
				Value value = ValueFactory.Create(util.GetFromField(instance,
						field));
				datafieldsToFamilyQualifierValue.get(field).Add(
						fq.getQualifier(), value);
			} else {
				// user defined class or a list as family data <br/>
				// 1. user defined class, need to add fixed qualifer informtion
				// to the fixedField
				if (fdt.isSubLevelClass()/* databasetable.canBeFamily() */) {
					Map<byte[], Value> qualifierValues = GetQualifierValuesFromInstanceAsFamily(
							util.GetFromField(instance, field), fq, fdt);
					datafieldsToFamilyQualifierValue.get(field).Add(
							qualifierValues);
				} else if (fdt.isList()/* databasefield.isQualiferList() */) {
					@SuppressWarnings("unchecked")
					List<String> list = (List<String>) (util.GetFromField(
							instance, field));

					if (list == null) {
						continue;
					}
					for (String key : list) {
						String qualifier = key;
						Value value = ValueFactory.Create(null);

						datafieldsToFamilyQualifierValue.get(field).Add(
								Bytes.toBytes(qualifier), value);
					}
				} else if (fdt.isMap()) {
					// 2. Map
					// TODO
				}

			}
		}
	}

	/**
	 * Just set the rowkey for the instance
	 * 
	 * @param instance
	 */
	public void SetRowKey(T instance) {
		for (Field field : instance.getClass().getDeclaredFields()) {
			// if is rowkey
			if (rowkeyField.equals(field)) {
				try {
					rowkey = ValueFactory.Create(util.GetFromField(instance,
							field));
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			break;
		}
	}

	public void SetFieldValue(T instance, List<String> fieldName)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException, HBaseOrmException {
		for (Field field : instance.getClass().getDeclaredFields()) {
			if (!fieldName.contains(field.getName())) {
				continue;
			}
			// if is rowkey
			if (rowkeyField.equals(field)) {
				rowkey = ValueFactory
						.Create(util.GetFromField(instance, field));
				continue;
			}
			FamilyQualifierSchema fq = fixedSchema.get(field);
			FieldDataType fdt = fieldDataType.get(field);
			// field not included for HBase
			if (fq == null) {
				continue;
			}

			// Primitive, family and qualifier name are both specified
			if (fq.getQualifier() != null) {
				Value value = ValueFactory.Create(util.GetFromField(instance,
						field));
				datafieldsToFamilyQualifierValue.get(field).Add(
						fq.getQualifier(), value);
			} else {
				// user defined class or a list as family data <br/>
				// 1. user defined class, need to add fixed qualifer informtion
				// to the fixedField
				if (fdt.isSubLevelClass()/* databasetable.canBeFamily() */) {
					Map<byte[], Value> qualifierValues = GetQualifierValuesFromInstanceAsFamily(
							util.GetFromField(instance, field), fq, fdt);
					datafieldsToFamilyQualifierValue.get(field).Add(
							qualifierValues);
				} else if (fdt.isList()/* databasefield.isQualiferList() */) {
					@SuppressWarnings("unchecked")
					List<String> list = (List<String>) (util.GetFromField(
							instance, field));

					if (list == null) {
						continue;
					}
					for (String key : list) {
						String qualifier = key;
						Value value = ValueFactory.Create(null);

						datafieldsToFamilyQualifierValue.get(field).Add(
								Bytes.toBytes(qualifier), value);
					}
				} else if (fdt.isMap()) {
					// 2. Map
					// TODO
				}

			}
		}

	}

	public void SetFieldValue(T instance, String fieldName, String subFieldName) {

	}

	/**
	 * 
	 */
	// public void SetA

	/**
	 * Build a map {qualifier: value} from the object as family
	 * 
	 * @param instance
	 *            the object as family
	 * @return
	 * @throws HBaseOrmException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	private Map<byte[], Value> GetQualifierValuesFromInstanceAsFamily(
			Object instance, FamilyQualifierSchema fqs, FieldDataType fdt)
			throws HBaseOrmException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		if (instance == null) {
			return null;
		}

		Map<byte[], Value> qualifierValues = new HashMap<byte[], Value>();
		{
			for (Field field : instance.getClass().getDeclaredFields()) {
				DatabaseField databaseField = field
						.getAnnotation(DatabaseField.class);
				if (fdt.isSkip()) {
					// not included in database
					continue;
				}
				Class<?> fieldType = field.getType();
				// 1. primitive type (actually include those UDF class, to which
				// we treat them as toString())
				if (fdt.getSubLevelDataType(field).isPrimitive()/*
																 * fieldType.
																 * isPrimitive()
																 */) {
					if (!fieldType.isPrimitive()) {
						LOG.warn("This is not good: instance is not primitive nor List nor Map , but "
								+ fieldType + ". We use toString() as value.");
					}
					String qualifier = getDatabaseColumnName(
							databaseField.qualifierName(), field);
					Value value = ValueFactory.Create(util.GetFromField(
							instance, field));
					qualifierValues.put(Bytes.toBytes(qualifier), value);

				}
				// Map, maybe HashMap or other map, all converted to Map
				else if (fdt.getSubLevelDataType(field).isMap()) {
					// get each key as qualifier and value as value
					@SuppressWarnings("unchecked")
					Map<String, Object> map = (Map<String, Object>) util
							.GetFromField(instance, field);
					for (String key : map.keySet()) {
						String qualifier = key;
						Value value = ValueFactory.Create(map.get(key));
						qualifierValues.put(Bytes.toBytes(qualifier), value);
					}

				}
				// List, maybe ArrayList or others list, all converted to List
				else if (fdt.getSubLevelDataType(field).isList()) {
					// not good ...
					@SuppressWarnings("unchecked")
					List<String> list = (List<String>) (util.GetFromField(
							instance, field));
					for (String key : list) {
						String qualifier = key;
						Value value = ValueFactory.Create(null);
						qualifierValues.put(Bytes.toBytes(qualifier), value);
					}
				} else {
					//
				}

			}
		}
		return qualifierValues;
	}

	private String getDatabaseColumnName(String string, Field field) {
		if (string.length() == 0) {
			LOG.info("Field "
					+ dataClass.getName()
					+ "."
					+ field.getName()
					+ " need to take care of ... field name is used as column name");
			return field.getName();
		}
		return string;
	}

}
