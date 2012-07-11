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
import org.apache.hadoop.hbase.util.Bytes;

import com.wlu.orm.hbase.annotation.DatabaseField;
import com.wlu.orm.hbase.annotation.DatabaseTable;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;

/**
 * This is factory of DataMapper, each Type can has one DataMapperFactory and
 * the factory can create DataMapper according to the instance
 * 
 * @author Administrator
 * 
 * @param <T>
 */
public class DataMapperFacory<T> {
	Log LOG = LogFactory.getLog(DataMapperFacory.class);
	public String tablename;
	public Map<Field, FamilyQualifierSchema> fixedSchema;
	public Field rowkeyField;
	public Class<?> dataClass;

	public DataMapperFacory(Class<T> dataClass_) throws HBaseOrmException {
		dataClass = dataClass_;
		// set tablename
		setTableName();
		// set fixed schema
		fixedSchema = new HashMap<Field, FamilyQualifierSchema>();
		setFixedSchema();
	}

	public DataMapper<T> Create(T instance) throws HBaseOrmException,
			IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		// check type
		if (!instance.getClass().equals(dataClass)) {
			return null;
		}
		// a new DataMapper constructed with the fixed members
		DataMapper<T> dm = new DataMapper<T>(tablename, fixedSchema,
				rowkeyField, dataClass);
		// 1. copy the fixed schema to datafieldToSchema. </br>
		// 2. fill value according to ... to Value of datafieldToSchema; </br>
		// notice:
		dm.CopyToDataFieldSchemaFromFixedSchema();
		dm.CopyToDataFieldsFromInstance(instance);
		return dm;
	}

	/**
	 * a helper method to return script to create the HBase table according to
	 * fixedSchema
	 * 
	 * @return Script to create create the table
	 */
	public String TableCreateScript() {
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

	public HTableDescriptor TableCreateDescriptor() {

		HTableDescriptor td = new HTableDescriptor(Bytes.toBytes(tablename));
		for (Field field : fixedSchema.keySet()) {
			FamilyQualifierSchema sc = fixedSchema.get(field);
			td.addFamily(new HColumnDescriptor(sc.getFamily()));
		}

		return td;

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
		Map<String, byte[]> subFieldToQualifier = null;
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
			// check whether is a sub level class as family
			DatabaseTable subdatabasetable = (DatabaseTable) field.getType()
					.getAnnotation(DatabaseTable.class);
			if (subdatabasetable != null && subdatabasetable.canBeFamily()) {
				for (Field subfield : field.getType().getDeclaredFields()) {
					DatabaseField subdatabasefield = subfield
							.getAnnotation(DatabaseField.class);
					if (subdatabasefield == null) {
						continue;
					}
					String subqualifiername = getDatabaseColumnName(
							subdatabasefield.qualifierName(), subfield);
					if (subFieldToQualifier == null) {
						subFieldToQualifier = new HashMap<String, byte[]>();
					}
					subFieldToQualifier.put(subfield.getName(),
							Bytes.toBytes(subqualifiername));
				}
			}
		}

		// TODO
		FamilyQualifierSchema fqv = new FamilyQualifierSchema();

		fqv.setFamily(Bytes.toBytes(family));
		if (qualifier == null) {
			fqv.setQualifier(null);
		} else {
			fqv.setQualifier(Bytes.toBytes(qualifier));
		}
		fqv.setSubFieldToQualifier(subFieldToQualifier);

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
