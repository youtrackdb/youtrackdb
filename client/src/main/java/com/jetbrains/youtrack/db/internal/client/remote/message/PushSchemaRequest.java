package com.jetbrains.youtrack.db.internal.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol.REQUEST_PUSH_SCHEMA;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.client.remote.RemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class PushSchemaRequest implements BinaryPushRequest<BinaryPushResponse> {

  private EntityImpl schema;

  public PushSchemaRequest() {
  }

  public PushSchemaRequest(EntityImpl schema) {
    this.schema = schema;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel)
      throws IOException {
    try {
      schema.setup(session);
      channel.writeBytes(RecordSerializerNetworkV37.INSTANCE.toStream(session, schema));
    } catch (IOException e) {
      throw BaseException.wrapException(new DatabaseException("Error on sending schema updates"),
          e);
    }
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.schema = (EntityImpl) RecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes,
        null);
  }

  @Override
  public BinaryPushResponse execute(DatabaseSessionInternal session,
      RemotePushHandler pushHandler) {
    return pushHandler.executeUpdateSchema(this);
  }

  @Override
  public BinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_SCHEMA;
  }

  public EntityImpl getSchema() {
    return schema;
  }
}
