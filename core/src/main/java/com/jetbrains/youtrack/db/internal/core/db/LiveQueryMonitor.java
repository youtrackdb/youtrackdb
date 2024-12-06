package com.jetbrains.youtrack.db.internal.core.db;

/**
 *
 */
public interface LiveQueryMonitor {

  void unSubscribe();

  int getMonitorId();
}
