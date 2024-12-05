package com.jetbrains.youtrack.db.internal.core.command.script.transformer;

import com.jetbrains.youtrack.db.internal.core.command.script.transformer.result.OResultTransformer;
import com.jetbrains.youtrack.db.internal.core.command.script.transformer.resultset.OResultSetTransformer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OScriptTransformer {

  YTResultSet toResultSet(YTDatabaseSessionInternal db, Object value);

  YTResult toResult(YTDatabaseSessionInternal db, Object value);

  boolean doesHandleResult(Object value);

  void registerResultTransformer(Class clazz, OResultTransformer resultTransformer);

  void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer);
}
