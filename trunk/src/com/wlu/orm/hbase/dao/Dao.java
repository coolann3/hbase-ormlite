package com.wlu.orm.hbase.dao;

import com.wlu.orm.hbase.schema.value.Value;

public interface Dao<T> {

	/**
	 * Create a HBase Table according to it's annotations. <br>
	 * If the table already exits, delete and then recreate.
	 * 
	 * @param clazz
	 */
	public void Create();

	/**
	 * Create a HBase Table according to it's annotations. <br>
	 * If the table already exits, return.
	 * 
	 * @param clazz
	 */
	public void CreateIfNotExist();

	/**
	 * Insert one record (row) to HBase table
	 * 
	 * @param data
	 */
	public void Insert(T data);

	public void DeleteById(Value rowkey);

	/**
	 * delete the whole data from HBase. (delete the row with data's rowkey)
	 * 
	 * @param data
	 */
	public void DeleteById(T data);
	
	/**
	 * Specify field name and delete specific family:qualifier 
	 * @param data
	 * @param family
	 * @param qualifier
	 */
	public void Delete(T data, String familyFieldName, String qualifierFieldName);
	
	/**
	 * Specify field name and delete whole specific family 
	 * @param data
	 * @param family
	 * @param qualifier
	 */
	public void Delete(T data, String familyFieldName);
	
	/**
	 * Same as DeleteById(T data)
	 * @param data
	 */
	public void Delete(T data);
	
	//public void Update(T data, String familyFieldName);
}
