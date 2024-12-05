package com.orientechnologies.core.sql.query;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.command.OCommandResultListener;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.ORecordOperation;

/**
 *
 */
public class OLocalLiveResultListener implements OLiveResultListener, OCommandResultListener {

  private final OLiveResultListener underlying;

  protected OLocalLiveResultListener(OLiveResultListener underlying) {
    this.underlying = underlying;
  }

  @Override
  public boolean result(YTDatabaseSessionInternal querySession, Object iRecord) {
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
  public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws YTException {
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
