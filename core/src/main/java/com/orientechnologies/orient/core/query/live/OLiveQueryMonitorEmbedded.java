package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/**
 * Created by luigidellaquila on 15/06/17.
 */
public class OLiveQueryMonitorEmbedded implements OLiveQueryMonitor {

  private final int token;
  private final ODatabaseSessionInternal db;

  public OLiveQueryMonitorEmbedded(int token, ODatabaseSessionInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    ODatabaseSessionInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
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
