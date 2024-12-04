package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.sql.executor.OResult;

/**
 *
 */
public interface OLiveQueryResultListener {

  void onCreate(YTDatabaseSession database, OResult data);

  void onUpdate(YTDatabaseSession database, OResult before, OResult after);

  void onDelete(YTDatabaseSession database, OResult data);

  void onError(YTDatabaseSession database, YTException exception);

  void onEnd(YTDatabaseSession database);
}
