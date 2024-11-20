package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;

/**
 * Created by tglman on 28/05/17.
 */
public class OLiveQueryClientListener {

  private final ODatabaseSession database;
  private final OLiveQueryResultListener listener;

  public OLiveQueryClientListener(ODatabaseSession database, OLiveQueryResultListener listener) {
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
    ODatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
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

  public void onError(OException e) {
    ODatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onError(database, e);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(old);
    }
  }

  public void onEnd() {
    ODatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onEnd(database);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(old);
    }
  }
}
