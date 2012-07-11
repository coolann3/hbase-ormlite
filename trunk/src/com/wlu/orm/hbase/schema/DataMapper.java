package com.wlu.orm.hbase.schema;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
			Map<Field, FamilyQualifierSchema> fixedSchema, Field rowkeyField,
			Class<?> dataClass) {
		this.tablename = tablename;
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
			FamilyQualifierSchema fqv = fixedSchema.get(field);
			if (fqv.getQualifier() != null) {
				// Primitive, family and qualifier name are both specified
				Value value = ValueFactory.Create(util.GetFromField(instance,
						field));
				datafieldsToFamilyQualifierValue.get(field).Add(
						fqv.getQualifier(), value);
			} else {
				// user defined class or a list as family data <br/>
				// 1. user defined class, need to add fixed qualifer informtion to the fixedField
				Map<byte[], Value> qualifierValues = GetQualifierValuesFromInstanceAsFamily(util
						.GetFromField(instance, field));
				datafieldsToFamilyQualifierValue.get(field)
						.Add(qualifierValues);
				// 2. Map or list
				// TODO
			}
		}
	}

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
			Object instance) throws HBaseOrmException,
			IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		if (instance == null) {
			return null;
		}
		DatabaseTable databaseTable = instance.getClass().getAnnotation(
				DatabaseTable.class);
		Map<byte[], Value> qualifierValues = new HashMap<byte[], Value>();
		if (!databaseTable.canBeFamily()) {
			return null;
		} else {
			for (Field field : instance.getClass().getDeclaredFields()) {
				DatabaseField databaseField = field
						.getAnnotation(DatabaseField.class);
				if (databaseField == null) {
					// not included in database
					continue;
				}
				Class<?> fieldType = field.getType();
				// 1. primitive type
				if (fieldType.isPrimitive()) {

					String qualifier = getDatabaseColumnName(
							databaseField.qualifierName(), field);
					Value value = ValueFactory.Create(util.GetFromField(
							instance, field));
					qualifierValues.put(Bytes.toBytes(qualifier), value);

				}
				// Map, TODO: maybe HashMap or other
				else if (fieldType.equals(Map.class)) {
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
				// List, TODO:: maybe ArrayList or others
				else if (fieldType.equals(List.class)) {
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
					// not good, use toString
					LOG.warn("This is not good: instance is not primitive nor List nor Map , but "
							+ fieldType + ". We use toString() as value.");
					String qualifier = getDatabaseColumnName(
							databaseField.qualifierName(), field);
					Value value = ValueFactory.Create(util.GetFromField(
							instance, field));
					qualifierValues.put(Bytes.toBytes(qualifier), value);
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
