package com.jetbrains.youtrack.db.internal.core.sql.query;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;

/**
 *
 */
public class LocalLiveResultListener implements LiveResultListener, CommandResultListener {

  private final LiveResultListener underlying;

  protected LocalLiveResultListener(LiveResultListener underlying) {
    this.underlying = underlying;
  }

  @Override
  public boolean result(DatabaseSessionInternal querySession, Object iRecord) {
    return false;
  }

  @Override
  public void end() {
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void onLiveResult(int iLiveToken, RecordOperation iOp) throws BaseException {
    underlying.onLiveResult(iLiveToken, iOp);
  }

  @Override
  public void onError(int iLiveToken) {
    underlying.onError(iLiveToken);
  }

  @Override
  public void onUnsubscribe(int iLiveToken) {
    underlying.onUnsubscribe(iLiveToken);
  }
}
