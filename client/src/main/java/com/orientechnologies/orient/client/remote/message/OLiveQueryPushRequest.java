package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OLiveQueryPushRequest implements OBinaryPushRequest {

  public static final byte HAS_MORE = 1;
  public static final byte END = 2;
  public static final byte ERROR = 3;

  private int monitorId;
  private byte status;
  private int errorIdentifier;
  private ErrorCode errorCode;
  private String errorMessage;
  private List<OLiveQueryResult> events;

  public OLiveQueryPushRequest(
      int monitorId, int errorIdentifier, ErrorCode errorCode, String errorMessage) {
    this.monitorId = monitorId;
    this.status = ERROR;
    this.errorIdentifier = errorIdentifier;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public OLiveQueryPushRequest(int monitorId, byte status, List<OLiveQueryResult> events) {
    this.monitorId = monitorId;
    this.status = status;
    this.events = events;
  }

  public OLiveQueryPushRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel)
      throws IOException {
    channel.writeInt(monitorId);
    channel.writeByte(status);
    if (status == ERROR) {
      channel.writeInt(errorIdentifier);
      channel.writeInt(errorCode.getCode());
      channel.writeString(errorMessage);
    } else {
      channel.writeInt(events.size());
      for (OLiveQueryResult event : events) {
        channel.writeByte(event.getEventType());
        OMessageHelper.writeResult(session,
            event.getCurrentValue(), channel, RecordSerializerNetworkV37.INSTANCE);
        if (event.getEventType() == OLiveQueryResult.UPDATE_EVENT) {
          OMessageHelper.writeResult(session,
              event.getOldValue(), channel, RecordSerializerNetworkV37.INSTANCE);
        }
      }
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network) throws IOException {
    monitorId = network.readInt();
    status = network.readByte();
    if (status == ERROR) {
      errorIdentifier = network.readInt();
      errorCode = ErrorCode.getErrorCode(network.readInt());
      errorMessage = network.readString();
    } else {
      int eventSize = network.readInt();
      events = new ArrayList<>(eventSize);
      while (eventSize-- > 0) {
        byte type = network.readByte();
        Result currentValue = OMessageHelper.readResult(db, network);
        Result oldValue = null;
        if (type == OLiveQueryResult.UPDATE_EVENT) {
          oldValue = OMessageHelper.readResult(db, network);
        }
        events.add(new OLiveQueryResult(type, currentValue, oldValue));
      }
    }
  }

  @Override
  public OBinaryPushResponse execute(DatabaseSessionInternal session, ORemotePushHandler remote) {
    remote.executeLiveQueryPush(this);
    return null;
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return ChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY;
  }

  public int getMonitorId() {
    return monitorId;
  }

  public List<OLiveQueryResult> getEvents() {
    return events;
  }

  public byte getStatus() {
    return status;
  }

  public void setStatus(byte status) {
    this.status = status;
  }

  public int getErrorIdentifier() {
    return errorIdentifier;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
