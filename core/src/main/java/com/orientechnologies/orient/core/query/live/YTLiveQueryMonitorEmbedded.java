package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YTLiveQueryMonitor;

/**
 *
 */
public class YTLiveQueryMonitorEmbedded implements YTLiveQueryMonitor {

  private final int token;
  private final YTDatabaseSessionInternal db;

  public YTLiveQueryMonitorEmbedded(int token, YTDatabaseSessionInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    YTDatabaseSessionInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
    db.activateOnCurrentThread();
    OLiveQueryHookV2.unsubscribe(token, db);
    if (prev != null) {
      ODatabaseRecordThreadLocal.instance().set(prev);
    } else {
      ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  @Override
  public int getMonitorId() {
    return token;
  }
}
