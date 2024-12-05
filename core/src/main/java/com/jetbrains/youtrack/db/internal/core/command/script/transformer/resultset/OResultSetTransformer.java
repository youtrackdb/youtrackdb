package com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset;

import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OResultSetTransformer<T> {

  YTResultSet transform(T value);
}
