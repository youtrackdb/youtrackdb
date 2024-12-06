package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
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
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    final EntityImpl result = new EntityImpl();
    result.field("databases", databases);
    byte[] toSend = serializer.toStream(session, result);
    channel.writeBytes(toSend);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    RecordSerializer serializer = RecordSerializerNetworkFactory.INSTANCE.current();
    final EntityImpl result = new EntityImpl();
    serializer.fromStream(db, network.readBytes(), result, null);
    databases = result.field("databases");
  }

  public Map<String, String> getDatabases() {
    return databases;
  }
}
