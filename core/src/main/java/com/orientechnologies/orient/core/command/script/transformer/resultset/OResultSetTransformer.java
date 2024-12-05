package com.orientechnologies.orient.core.command.script.transformer.resultset;

import com.orientechnologies.orient.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OResultSetTransformer<T> {

  YTResultSet transform(T value);
}
