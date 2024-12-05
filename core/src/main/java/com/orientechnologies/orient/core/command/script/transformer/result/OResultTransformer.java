package com.orientechnologies.orient.core.command.script.transformer.result;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.YTResult;

/**
 *
 */
public interface OResultTransformer<T> {

  YTResult transform(YTDatabaseSessionInternal db, T value);
}
