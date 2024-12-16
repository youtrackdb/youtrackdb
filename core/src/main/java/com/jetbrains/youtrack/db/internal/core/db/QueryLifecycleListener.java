package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.api.query.ResultSet;

/**
 *
 */
public interface QueryLifecycleListener {

  void queryStarted(String id, ResultSet resultSet);

  void queryClosed(String id);
}
