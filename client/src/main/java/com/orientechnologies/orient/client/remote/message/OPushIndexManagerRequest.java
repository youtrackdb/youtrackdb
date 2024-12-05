package com.orientechnologies.orient.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushIndexManagerRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private EntityImpl indexManager;

  public OPushIndexManagerRequest() {
  }

  public OPushIndexManagerRequest(EntityImpl indexManager) {
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
        (EntityImpl) ORecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes, null);
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

  public EntityImpl getIndexManager() {
    return indexManager;
  }
}
