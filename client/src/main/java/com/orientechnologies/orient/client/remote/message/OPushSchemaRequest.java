package com.orientechnologies.orient.client.remote.message;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_SCHEMA;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushSchemaRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private YTDocument schema;

  public OPushSchemaRequest() {
  }

  public OPushSchemaRequest(YTDocument schema) {
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
    this.schema = (YTDocument) ORecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes,
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

  public YTDocument getSchema() {
    return schema;
  }
}
