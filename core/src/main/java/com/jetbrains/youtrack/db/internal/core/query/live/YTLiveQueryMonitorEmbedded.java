package com.jetbrains.youtrack.db.internal.core.query.live;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;

/**
 *
 */
public class YTLiveQueryMonitorEmbedded implements LiveQueryMonitor {

  private final int token;
  private final DatabaseSessionInternal db;

  public YTLiveQueryMonitorEmbedded(int token, DatabaseSessionInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    var prev = DatabaseRecordThreadLocal.instance().getIfDefined();
    db.activateOnCurrentThread();
    LiveQueryHookV2.unsubscribe(token, db);
    if (prev != null) {
      DatabaseRecordThreadLocal.instance().set(prev);
    } else {
      DatabaseRecordThreadLocal.instance().remove();
    }
  }

  @Override
  public int getMonitorId() {
    return token;
  }
}
