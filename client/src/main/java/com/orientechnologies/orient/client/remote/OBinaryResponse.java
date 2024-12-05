package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryResponse {

  void write(YTDatabaseSessionInternal session, OChannelDataOutput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException;

  void read(YTDatabaseSessionInternal db, final OChannelDataInput network,
      OStorageRemoteSession session) throws IOException;
}
