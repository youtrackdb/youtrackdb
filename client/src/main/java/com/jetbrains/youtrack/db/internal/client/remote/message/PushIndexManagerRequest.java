package com.jetbrains.youtrack.db.internal.client.remote.message;

import static com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.client.remote.RemotePushHandler;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class PushIndexManagerRequest implements BinaryPushRequest<BinaryPushResponse> {

  private EntityImpl indexManager;

  public PushIndexManagerRequest() {
  }

  public PushIndexManagerRequest(EntityImpl indexManager) {
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
  public BinaryPushResponse execute(DatabaseSessionInternal session,
      RemotePushHandler pushHandler) {
    return pushHandler.executeUpdateIndexManager(this);
  }

  @Override
  public BinaryPushResponse createResponse() {
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
