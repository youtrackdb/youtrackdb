package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.StorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class OSubscribeLiveQueryRequest implements OBinaryRequest<OSubscribeLiveQueryResponse> {

  private String query;
  private Map<String, Object> params;
  private boolean namedParams;

  public OSubscribeLiveQueryRequest(String query, Map<String, Object> params) {
    this.query = query;
    this.params = params;
    this.namedParams = true;
  }

  public OSubscribeLiveQueryRequest(String query, Object[] params) {
    this.query = query;
    this.params = StorageRemote.paramsArrayToParamsMap(params);
    this.namedParams = false;
  }

  public OSubscribeLiveQueryRequest() {
  }

  @Override
  public void write(DatabaseSessionInternal database, ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    RecordSerializerNetworkV37Client serializer = new RecordSerializerNetworkV37Client();
    network.writeString(query);
    // params
    EntityImpl parms = new EntityImpl();
    parms.field("params", this.params);

    byte[] bytes = OMessageHelper.getRecordBytes(database, parms, serializer);
    network.writeBytes(bytes);
    network.writeBoolean(namedParams);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException {
    this.query = channel.readString();
    EntityImpl paramsDoc = new EntityImpl();
    byte[] bytes = channel.readBytes();
    serializer.fromStream(db, bytes, paramsDoc, null);
    this.params = paramsDoc.field("params");
    this.namedParams = channel.readBoolean();
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.SUBSCRIBE_PUSH_LIVE_QUERY;
  }

  @Override
  public OSubscribeLiveQueryResponse createResponse() {
    return new OSubscribeLiveQueryResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribeLiveQuery(this);
  }

  @Override
  public String getDescription() {
    return null;
  }

  public String getQuery() {
    return query;
  }

  public Map<String, Object> getParams() {
    return params;
  }
}
