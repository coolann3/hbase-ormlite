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

public class DataMapper<T> {
	Log LOG = LogFactory.getLog(DataMapper.class);
	// fixed schema for the generic type T
	public static String tablename;
	public static Map<Field, FamilyQualifierSchema> fixedSchema;
	public static Field rowkeyField;

	public static Class<?> dataClass;

	// private data for individual instance
	private Map<Field, FamilytoQualifersAndValues> datafieldsToFamilyQualifierValue;
	// private data for rowkey
	private Value rowkey;

	public DataMapper(Class<T> dataClass_) throws HBaseOrmException {
		dataClass = dataClass_;
		// set tablename
		setTableName();
		// set fixed schema
		fixedSchema = new HashMap<Field, FamilyQualifierSchema>();
		setFixedSchema();
	}

	public DataMapper(T instance) throws HBaseOrmException,
			IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		// 1. copy the fixed schema to datafieldToSchema. </br>
		// 2. fill value according to ... to Value of datafieldToSchema; </br>
		// notice:
		CopyToDataFieldSchemaFromFixedSchema();
		CopyToDataFieldsFromInstance(instance);

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
	 * a helper method to return script to create the HBase table according to
	 * fixedSchema
	 * 
	 * @return Script to create create the table
	 */
	public static String TableCreateScript() {
		StringBuffer sb = new StringBuffer();
		sb.append("create '");
		sb.append(tablename + "', ");
		for (Field field : fixedSchema.keySet()) {
			FamilyQualifierSchema sc = fixedSchema.get(field);
			String family = Bytes.toString(sc.getFamily());
			sb.append("{NAME => '" + family + "'},");
		}

		return sb.toString().substring(0, sb.length() - 1);
	}

	public static HTableDescriptor TableCreateDescriptor() {

		HTableDescriptor td = new HTableDescriptor(Bytes.toBytes(tablename));
		for (Field field : fixedSchema.keySet()) {
			FamilyQualifierSchema sc = fixedSchema.get(field);
			td.addFamily(new HColumnDescriptor(sc.getFamily()));
		}

		return td;

	}

	private void CopyToDataFieldSchemaFromFixedSchema()
			throws HBaseOrmException {
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
	private void CopyToDataFieldsFromInstance(T instance)
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
				// 1. user defined class
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

	/**
	 * if annotation is not set, use class name instead
	 */
	private void setTableName() {
		DatabaseTable databaseTable = (DatabaseTable) dataClass
				.getAnnotation(DatabaseTable.class);
		if (databaseTable == null || databaseTable.tableName().length() == 0) {
			LOG.warn("Table name is not specified as annotation, use class name instead");
			tablename = dataClass.getSimpleName();
		} else {
			tablename = databaseTable.tableName();
		}
	}

	/**
	 * <li>if annotation for a field is not set, the field is omitted. <br> <li>
	 * if is a rowkey; <br> <li>
	 * others
	 * 
	 * @throws HBaseOrmException
	 */
	private void setFixedSchema() throws HBaseOrmException {
		// TODO: maybe need to deal with inheritance scenario: dataClass's
		// super-class
		for (Field field : dataClass.getDeclaredFields()) {
			DatabaseField databaseField = field
					.getAnnotation(DatabaseField.class);
			if (databaseField == null) {
				// not included in database
				continue;
			}
			if (databaseField.id()) {
				// set the field as id
				rowkeyField = field;
				continue;
			}

			FamilyQualifierSchema fqv = FQSchemaBuildFromField(databaseField,
					field);

			fixedSchema.put(field, fqv);
		}
	}

	/**
	 * 
	 * @param databaseField
	 * @param field
	 * @return
	 * @throws HBaseOrmException
	 */
	private FamilyQualifierSchema FQSchemaBuildFromField(
			DatabaseField databaseField, Field field) throws HBaseOrmException {

		String family;
		String qualifier;
		// TODO
		// 1. primitive type
		if (field.getType().isPrimitive()) {
			if (databaseField.familyName().length() == 0) {
				throw new HBaseOrmException("primitive typed field "
						+ dataClass.getName() + "." + field.getName()
						+ " must define family with annotation.");
			} else {
				family = getDatabaseColumnName(databaseField.familyName(),
						field);
				qualifier = getDatabaseColumnName(
						databaseField.qualifierName(), field);
			}
		}
		// List, TODO: maybe ArrayList or other
		else if (field.getType().equals(List.class)) {
			// only set family, qualifier is ...
			family = getDatabaseColumnName(databaseField.familyName(), field);
			qualifier = null;
			if (!databaseField.isQualiferList()) {
				LOG.warn("Field " + field.getName()
						+ " should be annotated as 'isQualifierList = true '");
			}
		}
		// others
		else {
			// non-primitive and not List
			family = getDatabaseColumnName(databaseField.familyName(), field);
			qualifier = null;
		}

		// TODO
		FamilyQualifierSchema fqv = new FamilyQualifierSchema();

		fqv.setFamily(Bytes.toBytes(family));
		if (qualifier == null) {
			fqv.setQualifier(null);
		} else {
			fqv.setQualifier(Bytes.toBytes(qualifier));
		}

		return fqv;

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
