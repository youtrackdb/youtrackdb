package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.BinaryResponse;
import com.jetbrains.youtrack.db.internal.client.remote.StorageRemoteSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public class ListDatabasesResponse implements BinaryResponse {

  private Map<String, String> databases;

  public ListDatabasesResponse(Map<String, String> databases) {
    this.databases = databases;
  }

  public ListDatabasesResponse() {
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel,
      int protocolVersion, RecordSerializer serializer)
      throws IOException {
    final EntityImpl result = new EntityImpl(session);
    result.field("databases", databases);
    byte[] toSend = serializer.toStream(session, result);
    channel.writeBytes(toSend);
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network,
      StorageRemoteSession session) throws IOException {
    RecordSerializer serializer = RecordSerializerNetworkFactory.INSTANCE.current();
    final EntityImpl result = new EntityImpl(db);
    serializer.fromStream(db, network.readBytes(), result, null);
    databases = result.field("databases");
  }

  public Map<String, String> getDatabases() {
    return databases;
  }
}
