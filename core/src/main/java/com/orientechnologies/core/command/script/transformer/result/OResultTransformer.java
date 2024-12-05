package com.orientechnologies.core.command.script.transformer.result;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.sql.executor.YTResult;

/**
 *
 */
public interface OResultTransformer<T> {

  YTResult transform(YTDatabaseSessionInternal db, T value);
}
