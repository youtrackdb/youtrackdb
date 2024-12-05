package com.jetbrains.youtrack.db.internal.enterprise.channel.binary;

import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public interface OChannelDataInput {

  byte readByte() throws IOException;

  boolean readBoolean() throws IOException;

  int readInt() throws IOException;

  long readLong() throws IOException;

  short readShort() throws IOException;

  String readString() throws IOException;

  byte[] readBytes() throws IOException;

  YTRecordId readRID() throws IOException;

  int readVersion() throws IOException;

  InputStream getDataInput();
}
