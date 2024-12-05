package com.orientechnologies.core.command.script.transformer;

import com.orientechnologies.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;

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
