package com.orientechnologies.orient.server.network;

import com.jetbrains.youtrack.db.internal.core.config.YTContextConfiguration;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelBinary;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 *
 */
public class MockPipeChannel extends OChannelBinary {

  public MockPipeChannel(InputStream in, OutputStream out) throws IOException {
    super(new Socket(), new YTContextConfiguration());
    this.in = new DataInputStream(in);
    this.out = new DataOutputStream(out);
  }
}
