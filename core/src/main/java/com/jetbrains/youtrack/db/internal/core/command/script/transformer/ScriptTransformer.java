package com.jetbrains.youtrack.db.internal.core.command.script.transformer;

import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.ResultTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset.ResultSetTransformer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;

/**
 *
 */
public interface ScriptTransformer {

  ResultSet toResultSet(DatabaseSessionInternal db, Object value);

  Result toResult(DatabaseSessionInternal db, Object value);

  boolean doesHandleResult(Object value);

  void registerResultTransformer(Class clazz, ResultTransformer resultTransformer);

  void registerResultSetTransformer(Class clazz, ResultSetTransformer transformer);
}
