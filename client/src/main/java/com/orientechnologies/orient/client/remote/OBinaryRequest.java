package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import java.io.IOException;

/**
 *
 */
public interface OBinaryRequest<T extends OBinaryResponse> {

  void write(YTDatabaseSessionInternal database, final OChannelDataOutput network,
      OStorageRemoteSession session) throws IOException;

  void read(YTDatabaseSessionInternal db, OChannelDataInput channel, int protocolVersion,
      ORecordSerializer serializer)
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
