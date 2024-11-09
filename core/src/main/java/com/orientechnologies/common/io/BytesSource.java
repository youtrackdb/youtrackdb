package com.orientechnologies.common.io;

import java.io.IOException;

public interface BytesSource {

  int availableBytes(int offset);

  default int availableBytes() {
    return availableBytes(0);
  }

  default int availableBytes(int offset, int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("Limit cannot be negative");
    }
    return Math.min(availableBytes(offset), limit);
  }

  default boolean hasContent() {
    return availableBytes(0, 1) > 0;
  }

  boolean advance(int advanceBytes) throws IOException;

  void copyTo(byte[] dest, int destOffset, int len);

  void close() throws IOException;
}
