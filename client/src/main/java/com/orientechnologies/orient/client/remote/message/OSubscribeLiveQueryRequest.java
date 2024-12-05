package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.StorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
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
  public void write(YTDatabaseSessionInternal database, OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetworkV37Client serializer = new ORecordSerializerNetworkV37Client();
    network.writeString(query);
    // params
    EntityImpl parms = new EntityImpl();
    parms.field("params", this.params);

    byte[] bytes = OMessageHelper.getRecordBytes(database, parms, serializer);
    network.writeBytes(bytes);
    network.writeBoolean(namedParams);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
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
    return OChannelBinaryProtocol.SUBSCRIBE_PUSH_LIVE_QUERY;
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
