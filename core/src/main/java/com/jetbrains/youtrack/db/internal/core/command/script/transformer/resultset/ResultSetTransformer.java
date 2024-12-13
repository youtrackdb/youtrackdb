package com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset;

import com.jetbrains.youtrack.db.api.query.ResultSet;

/**
 *
 */
public interface ResultSetTransformer<T> {

  ResultSet transform(T value);
}
