package com.orientechnologies.orient.client.remote.message;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushIndexManagerRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private YTEntityImpl indexManager;

  public OPushIndexManagerRequest() {
  }

  public OPushIndexManagerRequest(YTEntityImpl indexManager) {
    this.indexManager = indexManager;
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel)
      throws IOException {
    indexManager.setup(session);
    channel.writeBytes(ORecordSerializerNetworkV37.INSTANCE.toStream(session, indexManager));
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.indexManager =
        (YTEntityImpl) ORecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes, null);
  }

  @Override
  public OBinaryPushResponse execute(YTDatabaseSessionInternal session,
      ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateIndexManager(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_INDEX_MANAGER;
  }

  public YTEntityImpl getIndexManager() {
    return indexManager;
  }
}
