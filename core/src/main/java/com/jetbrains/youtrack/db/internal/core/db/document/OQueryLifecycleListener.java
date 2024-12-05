package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OQueryLifecycleListener {

  void queryStarted(String id, YTResultSet resultSet);

  void queryClosed(String id);
}
