package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;

/**
 *
 */
public interface YTLiveQueryResultListener {

  void onCreate(YTDatabaseSession database, YTResult data);

  void onUpdate(YTDatabaseSession database, YTResult before, YTResult after);

  void onDelete(YTDatabaseSession database, YTResult data);

  void onError(YTDatabaseSession database, YTException exception);

  void onEnd(YTDatabaseSession database);
}
