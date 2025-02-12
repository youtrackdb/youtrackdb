package com.jetbrains.youtrack.db.api.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public interface LiveQueryResultListener {

  void onCreate(DatabaseSessionInternal session, Result data);

  void onUpdate(DatabaseSessionInternal session, Result before, Result after);

  void onDelete(DatabaseSessionInternal session, Result data);

  void onError(DatabaseSession session, BaseException exception);

  void onEnd(DatabaseSession session);
}
