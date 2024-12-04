package com.orientechnologies.orient.core.command.script.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/**
 *
 */
public interface OScriptTransformer {

  OResultSet toResultSet(YTDatabaseSessionInternal db, Object value);

  OResult toResult(YTDatabaseSessionInternal db, Object value);

  boolean doesHandleResult(Object value);

  void registerResultTransformer(Class clazz, OResultTransformer resultTransformer);

  void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer);
}
