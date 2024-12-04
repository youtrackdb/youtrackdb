package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;

/**
 *
 */
public interface OValuesConverter<T> {

  T convert(YTDatabaseSessionInternal db, T value);
}
