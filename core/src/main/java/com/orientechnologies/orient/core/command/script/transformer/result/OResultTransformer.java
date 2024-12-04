package com.orientechnologies.orient.core.command.script.transformer.result;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.OResult;

/**
 *
 */
public interface OResultTransformer<T> {

  OResult transform(YTDatabaseSessionInternal db, T value);
}
