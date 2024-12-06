package com.jetbrains.youtrack.db.internal.core.command.script.transformer.result;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;

/**
 *
 */
public interface ResultTransformer<T> {

  Result transform(DatabaseSessionInternal db, T value);
}
