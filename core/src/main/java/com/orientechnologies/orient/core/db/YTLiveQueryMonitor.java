package com.orientechnologies.orient.core.db;

/**
 *
 */
public interface YTLiveQueryMonitor {

  void unSubscribe();

  int getMonitorId();
}
