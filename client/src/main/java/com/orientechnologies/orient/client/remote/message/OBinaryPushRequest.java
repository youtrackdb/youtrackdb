package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryPushRequest<T extends OBinaryPushResponse> {

  void write(YTDatabaseSessionInternal session, OChannelDataOutput channel) throws IOException;

  void read(YTDatabaseSessionInternal db, final OChannelDataInput network) throws IOException;

  T execute(YTDatabaseSessionInternal session, ORemotePushHandler remote);

  OBinaryPushResponse createResponse();

  byte getPushCommand();
}
