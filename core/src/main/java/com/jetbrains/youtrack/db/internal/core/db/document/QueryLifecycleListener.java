package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;

/**
 *
 */
public interface QueryLifecycleListener {

  void queryStarted(String id, ResultSet resultSet);

  void queryClosed(String id);
}
