package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import java.io.IOException;

/**
 *
 */
public interface OBinaryPushRequest<T extends OBinaryPushResponse> {

  void write(DatabaseSessionInternal session, ChannelDataOutput channel) throws IOException;

  void read(DatabaseSessionInternal db, final ChannelDataInput network) throws IOException;

  T execute(DatabaseSessionInternal session, ORemotePushHandler remote);

  OBinaryPushResponse createResponse();

  byte getPushCommand();
}
