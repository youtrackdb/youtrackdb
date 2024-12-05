package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.sql.executor.YTResult;

/**
 *
 */
public interface OLiveQueryResultListener {

  void onCreate(YTDatabaseSession database, YTResult data);

  void onUpdate(YTDatabaseSession database, YTResult before, YTResult after);

  void onDelete(YTDatabaseSession database, YTResult data);

  void onError(YTDatabaseSession database, YTException exception);

  void onEnd(YTDatabaseSession database);
}
