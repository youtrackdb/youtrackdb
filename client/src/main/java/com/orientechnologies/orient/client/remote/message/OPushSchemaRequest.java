package com.orientechnologies.orient.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_SCHEMA;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushSchemaRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private EntityImpl schema;

  public OPushSchemaRequest() {
  }

  public OPushSchemaRequest(EntityImpl schema) {
    this.schema = schema;
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel)
      throws IOException {
    try {
      schema.setup(session);
      channel.writeBytes(ORecordSerializerNetworkV37.INSTANCE.toStream(session, schema));
    } catch (IOException e) {
      throw YTException.wrapException(new YTDatabaseException("Error on sending schema updates"),
          e);
    }
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.schema = (EntityImpl) ORecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes,
        null);
  }

  @Override
  public OBinaryPushResponse execute(YTDatabaseSessionInternal session,
      ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateSchema(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
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
