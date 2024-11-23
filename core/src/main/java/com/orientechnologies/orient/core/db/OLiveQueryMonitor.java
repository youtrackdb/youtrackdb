package com.orientechnologies.orient.core.db;

/**
 *
 */
public interface OLiveQueryMonitor {

  void unSubscribe();

  int getMonitorId();
}
