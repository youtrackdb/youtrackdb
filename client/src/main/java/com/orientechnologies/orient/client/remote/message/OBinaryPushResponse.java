package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryPushResponse {

  void write(final ChannelDataOutput network) throws IOException;

  void read(ChannelDataInput channel) throws IOException;
}
