package com.orientechnologies.orient.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OPushIndexManagerRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private EntityImpl indexManager;

  public OPushIndexManagerRequest() {
  }

  public OPushIndexManagerRequest(EntityImpl indexManager) {
    this.indexManager = indexManager;
  }

  @Override
  public void write(DatabaseSessionInternal session, ChannelDataOutput channel)
      throws IOException {
    indexManager.setup(session);
    channel.writeBytes(RecordSerializerNetworkV37.INSTANCE.toStream(session, indexManager));
  }

  @Override
  public void read(DatabaseSessionInternal db, ChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.indexManager =
        (EntityImpl) RecordSerializerNetworkV37Client.INSTANCE.fromStream(db, bytes, null);
  }

  @Override
  public OBinaryPushResponse execute(DatabaseSessionInternal session,
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
