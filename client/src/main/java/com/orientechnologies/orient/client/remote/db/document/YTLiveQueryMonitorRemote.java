package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.core.db.YTLiveQueryMonitor;

/**
 *
 */
public class YTLiveQueryMonitorRemote implements YTLiveQueryMonitor {

  private final YTDatabaseSessionRemote database;
  private final int monitorId;

  public YTLiveQueryMonitorRemote(YTDatabaseSessionRemote database, int monitorId) {
    this.database = database;
    this.monitorId = monitorId;
  }

  @Override
  public void unSubscribe() {
    database.getStorageRemote().unsubscribeLive(database, this.monitorId);
  }

  @Override
  public int getMonitorId() {
    return monitorId;
  }
}
