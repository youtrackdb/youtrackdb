package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryPushRequest<T extends OBinaryPushResponse> {

  void write(ODatabaseSessionInternal session, OChannelDataOutput channel) throws IOException;

  void read(ODatabaseSessionInternal db, final OChannelDataInput network) throws IOException;

  T execute(ODatabaseSessionInternal session, ORemotePushHandler remote);

  OBinaryPushResponse createResponse();

  byte getPushCommand();
}
