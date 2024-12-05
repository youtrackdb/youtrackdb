package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YTLiveQueryResultListener;

/**
 *
 */
public class OLiveQueryClientListener {

  private final YTDatabaseSession database;
  private final YTLiveQueryResultListener listener;

  public OLiveQueryClientListener(YTDatabaseSession database, YTLiveQueryResultListener listener) {
    this.database = database;
    this.listener = listener;
  }

  /**
   * Return true if the push request require an unregister
   *
   * @param pushRequest
   * @return
   */
  public boolean onEvent(OLiveQueryPushRequest pushRequest) {
    YTDatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      if (pushRequest.getStatus() == OLiveQueryPushRequest.ERROR) {
        onError(pushRequest.getErrorCode().newException(pushRequest.getErrorMessage(), null));
        return true;
      } else {
        for (OLiveQueryResult result : pushRequest.getEvents()) {
          switch (result.getEventType()) {
            case OLiveQueryResult.CREATE_EVENT:
              listener.onCreate(database, result.getCurrentValue());
              break;
            case OLiveQueryResult.UPDATE_EVENT:
              listener.onUpdate(database, result.getOldValue(), result.getCurrentValue());
              break;
            case OLiveQueryResult.DELETE_EVENT:
              listener.onDelete(database, result.getCurrentValue());
              break;
          }
        }
        if (pushRequest.getStatus() == OLiveQueryPushRequest.END) {
          onEnd();
          return true;
        }
      }
      return false;
    } finally {
      ODatabaseRecordThreadLocal.instance().set(old);
    }
  }

  public void onError(YTException e) {
    YTDatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onError(database, e);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(old);
    }
  }

  public void onEnd() {
    YTDatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onEnd(database);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(old);
    }
  }
}
