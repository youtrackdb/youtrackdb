package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;

/**
 *
 */
public interface OValuesConverter<T> {

  T convert(ODatabaseSessionInternal db, T value);
}
