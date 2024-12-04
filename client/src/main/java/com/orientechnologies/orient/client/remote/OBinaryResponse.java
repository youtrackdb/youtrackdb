package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
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
