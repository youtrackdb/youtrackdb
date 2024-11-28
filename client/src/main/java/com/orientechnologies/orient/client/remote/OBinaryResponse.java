package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryResponse {

  void write(ODatabaseSessionInternal session, OChannelDataOutput channel, int protocolVersion,
      ORecordSerializer serializer)
      throws IOException;

  void read(ODatabaseSessionInternal db, final OChannelDataInput network,
      OStorageRemoteSession session) throws IOException;
}
