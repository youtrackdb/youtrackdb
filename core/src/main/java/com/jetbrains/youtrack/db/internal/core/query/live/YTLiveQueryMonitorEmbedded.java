package com.jetbrains.youtrack.db.internal.core.query.live;

import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

public class YTLiveQueryMonitorEmbedded implements LiveQueryMonitor {
  private final int token;
  private final DatabaseSessionInternal db;

  public YTLiveQueryMonitorEmbedded(int token, DatabaseSessionInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    db.activateOnCurrentThread();
    LiveQueryHookV2.unsubscribe(token, db);
  }

  @Override
  public int getMonitorId() {
    return token;
  }
}
