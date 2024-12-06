package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryResultListener;

/**
 *
 */
public class OLiveQueryClientListener {

  private final DatabaseSession database;
  private final LiveQueryResultListener listener;

  public OLiveQueryClientListener(DatabaseSession database, LiveQueryResultListener listener) {
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
    DatabaseSessionInternal old = DatabaseRecordThreadLocal.instance().getIfDefined();
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
      DatabaseRecordThreadLocal.instance().set(old);
    }
  }

  public void onError(BaseException e) {
    DatabaseSessionInternal old = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onError(database, e);
      database.close();
    } finally {
      DatabaseRecordThreadLocal.instance().set(old);
    }
  }

  public void onEnd() {
    DatabaseSessionInternal old = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onEnd(database);
      database.close();
    } finally {
      DatabaseRecordThreadLocal.instance().set(old);
    }
  }
}
