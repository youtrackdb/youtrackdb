package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.client.remote.message.LiveQueryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.live.LiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 *
 */
public class LiveQueryClientListener {

  private final DatabaseSessionInternal database;
  private final LiveQueryResultListener listener;

  public LiveQueryClientListener(DatabaseSessionInternal database,
      LiveQueryResultListener listener) {
    this.database = database;
    this.listener = listener;
  }

  /**
   * Return true if the push request require an unregister
   *
   * @param pushRequest
   * @return
   */
  public boolean onEvent(LiveQueryPushRequest pushRequest) {
    DatabaseSessionInternal old = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      database.activateOnCurrentThread();
      if (pushRequest.getStatus() == LiveQueryPushRequest.ERROR) {
        onError(pushRequest.getErrorCode().newException(pushRequest.getErrorMessage(), null));
        return true;
      } else {
        for (LiveQueryResult result : pushRequest.getEvents()) {
          switch (result.getEventType()) {
            case LiveQueryResult.CREATE_EVENT:
              listener.onCreate(database, result.getCurrentValue());
              break;
            case LiveQueryResult.UPDATE_EVENT:
              listener.onUpdate(database, result.getOldValue(), result.getCurrentValue());
              break;
            case LiveQueryResult.DELETE_EVENT:
              listener.onDelete(database, result.getCurrentValue());
              break;
          }
        }
        if (pushRequest.getStatus() == LiveQueryPushRequest.END) {
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
