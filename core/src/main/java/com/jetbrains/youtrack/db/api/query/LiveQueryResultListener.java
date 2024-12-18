package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public interface LiveQueryResultListener {

  void onCreate(DatabaseSessionInternal database, Result data);

  void onUpdate(DatabaseSessionInternal database, Result before, Result after);

  void onDelete(DatabaseSessionInternal database, Result data);

  void onError(DatabaseSession database, BaseException exception);

  void onEnd(DatabaseSession database);
}
