package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;

/**
 *
 */
public interface LiveQueryResultListener {

  void onCreate(DatabaseSession database, Result data);

  void onUpdate(DatabaseSession database, Result before, Result after);

  void onDelete(DatabaseSession database, Result data);

  void onError(DatabaseSession database, BaseException exception);

  void onEnd(DatabaseSession database);
}
