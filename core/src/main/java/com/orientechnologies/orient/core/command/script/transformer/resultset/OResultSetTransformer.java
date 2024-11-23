package com.orientechnologies.orient.core.command.script.transformer.resultset;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

/**
 *
 */
public interface OResultSetTransformer<T> {

  OResultSet transform(T value);
}
