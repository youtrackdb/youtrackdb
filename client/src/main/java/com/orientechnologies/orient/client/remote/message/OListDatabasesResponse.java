package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public class OListDatabasesResponse implements OBinaryResponse {

  private Map<String, String> databases;

  public OListDatabasesResponse(Map<String, String> databases) {
    this.databases = databases;
  }

  public OListDatabasesResponse() {
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    final YTEntityImpl result = new YTEntityImpl();
    result.field("databases", databases);
    byte[] toSend = serializer.toStream(session, result);
    channel.writeBytes(toSend);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkFactory.INSTANCE.current();
    final YTEntityImpl result = new YTEntityImpl();
    serializer.fromStream(db, network.readBytes(), result, null);
    databases = result.field("databases");
  }

  public Map<String, String> getDatabases() {
    return databases;
  }
}
