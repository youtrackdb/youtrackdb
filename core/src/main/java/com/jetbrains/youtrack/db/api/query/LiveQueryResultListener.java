package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;

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
