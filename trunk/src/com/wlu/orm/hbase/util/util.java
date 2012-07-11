package com.wlu.orm.hbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class util {
	private static String methodFromField(Field field, String prefix) {
		return prefix + field.getName().substring(0, 1).toUpperCase()
				+ field.getName().substring(1);
	}

	public static Method findGetMethod(Field field) {
		String methodName = methodFromField(field, "get");
		Method fieldGetMethod;
		try {
			fieldGetMethod = field.getDeclaringClass().getMethod(methodName);
		} catch (Exception e) {
			return null;
		}
		if (fieldGetMethod.getReturnType() != field.getType()) {
			return null;
		}
		return fieldGetMethod;
	}

	public static <T> Object GetFromField(T instance, Field field)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Method m = findGetMethod(field);
		return m.invoke(instance);
	}
}
