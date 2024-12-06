package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import java.io.IOException;

/**
 *
 */
public interface OBinaryRequest<T extends OBinaryResponse> {

  void write(DatabaseSessionInternal database, final ChannelDataOutput network,
      OStorageRemoteSession session) throws IOException;

  void read(DatabaseSessionInternal db, ChannelDataInput channel, int protocolVersion,
      RecordSerializer serializer)
      throws IOException;

  byte getCommand();

  T createResponse();

  OBinaryResponse execute(OBinaryRequestExecutor executor);

  String getDescription();

  default boolean requireServerUser() {
    return false;
  }

  default boolean requireDatabaseSession() {
    return true;
  }

  default String requiredServerRole() {
    return "";
  }
}
