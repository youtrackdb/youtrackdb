package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/**
 *
 */
public class OLiveQueryMonitorRemote implements OLiveQueryMonitor {

  private final YTDatabaseSessionRemote database;
  private final int monitorId;

  public OLiveQueryMonitorRemote(YTDatabaseSessionRemote database, int monitorId) {
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
