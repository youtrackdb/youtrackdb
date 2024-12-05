package com.orientechnologies.orient.server;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YTLiveQueryBatchResultListener;
import com.orientechnologies.orient.core.exception.YTCoreException;
import com.orientechnologies.orient.core.exception.YTLiveQueryInterruptedException;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
class YTServerLiveQueryResultListener implements YTLiveQueryBatchResultListener {

  private final ONetworkProtocolBinary protocol;
  private final OSharedContext sharedContext;
  private int monitorId;

  List<OLiveQueryResult> toSend = new ArrayList<>();

  public YTServerLiveQueryResultListener(
      ONetworkProtocolBinary protocol, OSharedContext sharedContext) {
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
  public void onCreate(YTDatabaseSession database, YTResult data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(YTDatabaseSession database, YTResult before, YTResult after) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(YTDatabaseSession database, YTResult data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, data, null));
  }

  @Override
  public void onError(YTDatabaseSession database, YTException exception) {
    try {
      // TODO: resolve error identifier
      int errorIdentifier = 0;
      OErrorCode code = OErrorCode.GENERIC_ERROR;
      if (exception instanceof YTCoreException) {
        code = ((YTCoreException) exception).getErrorCode();
      }
      protocol.push((YTDatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, errorIdentifier, code, exception.getMessage()));
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onEnd(YTDatabaseSession database) {
    try {
      protocol.push((YTDatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.END, Collections.emptyList()));
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onBatchEnd(YTDatabaseSession database) {
    sendEvents(database);
  }

  private synchronized void sendEvents(YTDatabaseSession database) {
    if (toSend.isEmpty()) {
      return;
    }
    List<OLiveQueryResult> events = toSend;
    toSend = new ArrayList<>();

    try {
      protocol.push((YTDatabaseSessionInternal) database,
          new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.HAS_MORE, events));
    } catch (IOException e) {
      sharedContext.getLiveQueryOpsV2().getSubscribers().remove(monitorId);
      throw YTException.wrapException(
          new YTLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }
}
