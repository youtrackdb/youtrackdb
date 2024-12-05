package com.orientechnologies.orient.core.command.script.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;

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
