package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.sql.executor.YTResultSet;

/**
 *
 */
public interface OQueryLifecycleListener {

  void queryStarted(String id, YTResultSet resultSet);

  void queryClosed(String id);
}
