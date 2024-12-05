package com.orientechnologies.core.db.tool.importer;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;

/**
 *
 */
public interface OValuesConverter<T> {

  T convert(YTDatabaseSessionInternal db, T value);
}
