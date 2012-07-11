package com.wlu.orm.hbase.schema;

import org.apache.hadoop.hbase.client.Put;

import com.wlu.orm.hbase.schema.value.Value;

public class FamilyQualifierSchema {
	private byte[] family = null;
	private byte[] qualifier = null;

	// TODO: maybe rmoved later
	private Value V = null;

	private byte[] value = null;

	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

//	public Value getValue() {
//		return V;
//	}
//
//	public void setValue(Value v) {
//		V = v;
//		value = v.toBytes();
//	}

	public Put AddToPut(Put put) {
		if (put == null) {
			return null;
		}
		put.add(family, qualifier, value);
		return put;
	}

}
