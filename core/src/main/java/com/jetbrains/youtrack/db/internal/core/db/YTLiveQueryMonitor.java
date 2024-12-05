package com.jetbrains.youtrack.db.internal.core.db;

/**
 *
 */
public interface YTLiveQueryMonitor {

  void unSubscribe();

  int getMonitorId();
}
