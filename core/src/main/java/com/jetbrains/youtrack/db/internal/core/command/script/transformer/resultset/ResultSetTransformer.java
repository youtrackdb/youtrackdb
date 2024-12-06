package com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset;

import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;

/**
 *
 */
public interface ResultSetTransformer<T> {

  ResultSet transform(T value);
}
