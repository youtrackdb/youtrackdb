package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface BinaryResponse {

  void write(DatabaseSessionInternal session, ChannelDataOutput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException;

  void read(DatabaseSessionInternal db, final ChannelDataInput network,
      StorageRemoteSession session) throws IOException;
}
