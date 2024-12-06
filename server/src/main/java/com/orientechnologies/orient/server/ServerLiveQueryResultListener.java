package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.exception.LiveQueryInterruptedException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.client.remote.message.LiveQueryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.live.LiveQueryResult;
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

  List<LiveQueryResult> toSend = new ArrayList<>();

  public ServerLiveQueryResultListener(
      NetworkProtocolBinary protocol, SharedContext sharedContext) {
    this.protocol = protocol;
    this.sharedContext = sharedContext;
  }

  public void setMonitorId(int monitorId) {
    this.monitorId = monitorId;
  }

  private synchronized void addEvent(LiveQueryResult event) {
    toSend.add(event);
  }

  @Override
  public void onCreate(DatabaseSession database, Result data) {
    addEvent(new LiveQueryResult(LiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(DatabaseSession database, Result before, Result after) {
    addEvent(new LiveQueryResult(LiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(DatabaseSession database, Result data) {
    addEvent(new LiveQueryResult(LiveQueryResult.DELETE_EVENT, data, null));
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
          new LiveQueryPushRequest(monitorId, errorIdentifier, code, exception.getMessage()));
    } catch (IOException e) {
      throw BaseException.wrapException(
          new LiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onEnd(DatabaseSession database) {
    try {
      protocol.push((DatabaseSessionInternal) database,
          new LiveQueryPushRequest(monitorId, LiveQueryPushRequest.END, Collections.emptyList()));
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
    List<LiveQueryResult> events = toSend;
    toSend = new ArrayList<>();

    try {
      protocol.push((DatabaseSessionInternal) database,
          new LiveQueryPushRequest(monitorId, LiveQueryPushRequest.HAS_MORE, events));
    } catch (IOException e) {
      sharedContext.getLiveQueryOpsV2().getSubscribers().remove(monitorId);
      throw BaseException.wrapException(
          new LiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }
}
