package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.exception.LiveQueryInterruptedException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryBatchResultListener;
import com.jetbrains.youtrack.db.internal.core.exception.CoreException;
import com.orientechnologies.orient.server.network.protocol.binary.NetworkProtocolBinary;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
class ServerLiveQueryResultListener implements LiveQueryBatchResultListener {

  private final NetworkProtocolBinary protocol;
  private final SharedContext sharedContext;
  private int monitorId;

  List<OLiveQueryResult> toSend = new ArrayList<>();

  public ServerLiveQueryResultListener(
      NetworkProtocolBinary protocol, SharedContext sharedContext) {
    this.protocol = protocol;
    this.sharedContext = sharedContext;
  }

  public void setMonitorId(int monitorId) {
    this.monitorId = monitorId;
  }

  private synchronized void addEvent(OLiveQueryResult event) {
    toSend.add(event);
  }

  @Override
  public void onCreate(DatabaseSession database, Result data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(DatabaseSession database, Result before, Result after) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(DatabaseSession database, Result data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, data, null));
  }

  @Override
  public void onError(DatabaseSession database, BaseException exception) {
    try {
      // TODO: resolve error identifier
      int errorIdentifier = 0;
      ErrorCode code = ErrorCode.GENERIC_ERROR;
      if (exception instanceof CoreException) {
        code = ((CoreException) exception).getErrorCode();
      }
      protocol.push((DatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, errorIdentifier, code, exception.getMessage()));
    } catch (IOException e) {
      throw BaseException.wrapException(
          new LiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onEnd(DatabaseSession database) {
    try {
      protocol.push((DatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.END, Collections.emptyList()));
    } catch (IOException e) {
      throw BaseException.wrapException(
          new LiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onBatchEnd(DatabaseSession database) {
    sendEvents(database);
  }

  private synchronized void sendEvents(DatabaseSession database) {
    if (toSend.isEmpty()) {
      return;
    }
    List<OLiveQueryResult> events = toSend;
    toSend = new ArrayList<>();

    try {
      protocol.push((DatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.HAS_MORE, events));
    } catch (IOException e) {
      sharedContext.getLiveQueryOpsV2().getSubscribers().remove(monitorId);
      throw BaseException.wrapException(
          new LiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }
}
