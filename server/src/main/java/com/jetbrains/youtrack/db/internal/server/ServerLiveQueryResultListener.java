package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.exception.LiveQueryInterruptedException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.client.remote.message.LiveQueryPushRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.live.LiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.LiveQueryBatchResultListener;
import com.jetbrains.youtrack.db.internal.core.exception.CoreException;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.NetworkProtocolBinary;
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
  public void onCreate(DatabaseSessionInternal database, Result data) {
    addEvent(new LiveQueryResult(LiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(DatabaseSessionInternal database, Result before, Result after) {
    addEvent(new LiveQueryResult(LiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(DatabaseSessionInternal database, Result data) {
    addEvent(new LiveQueryResult(LiveQueryResult.DELETE_EVENT, data, null));
  }

  @Override
  public void onError(DatabaseSession database, BaseException exception) {
    try {
      // TODO: resolve error identifier
      var errorIdentifier = 0;
      var code = ErrorCode.GENERIC_ERROR;
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
    var events = toSend;
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
