package com.jetbrains.youtrack.db.internal.core.command.script.transformer.result;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

/**
 *
 */
public interface OResultTransformer<T> {

  YTResult transform(YTDatabaseSessionInternal db, T value);
}
