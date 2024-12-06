package com.jetbrains.youtrack.db.internal.client.remote.db.document;

import com.jetbrains.youtrack.db.internal.core.db.LiveQueryMonitor;

/**
 *
 */
public class YTLiveQueryMonitorRemote implements LiveQueryMonitor {

  private final DatabaseSessionRemote database;
  private final int monitorId;

  public YTLiveQueryMonitorRemote(DatabaseSessionRemote database, int monitorId) {
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
