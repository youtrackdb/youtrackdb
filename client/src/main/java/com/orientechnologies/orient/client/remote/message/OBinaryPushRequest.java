package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
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
