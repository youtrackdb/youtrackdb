package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/**
 *
 */
public interface OBinaryPushResponse {

  void write(final OChannelDataOutput network) throws IOException;

  void read(OChannelDataInput channel) throws IOException;
}
