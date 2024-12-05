package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;

/**
 *
 */
public interface OValuesConverter<T> {

  T convert(YTDatabaseSessionInternal db, T value);
}
