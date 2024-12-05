package com.orientechnologies.core.db.document;

import com.orientechnologies.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OQueryLifecycleListener {

  void queryStarted(String id, YTResultSet resultSet);

  void queryClosed(String id);
}
