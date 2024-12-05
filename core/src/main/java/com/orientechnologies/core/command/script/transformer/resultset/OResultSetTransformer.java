package com.orientechnologies.core.command.script.transformer.resultset;

import com.orientechnologies.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OResultSetTransformer<T> {

  YTResultSet transform(T value);
}
