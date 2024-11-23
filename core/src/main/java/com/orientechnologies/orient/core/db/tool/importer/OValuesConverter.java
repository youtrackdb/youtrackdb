package com.orientechnologies.orient.core.db.tool.importer;

/**
 *
 */
public interface OValuesConverter<T> {

  T convert(T value);
}
