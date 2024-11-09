package com.orientechnologies.common.io;

import java.io.IOException;

public class ByteArraySource implements BytesSource {

  private final byte[] source;
  private int offset = 0;

  public ByteArraySource(byte[] source) {
    this.source = source;
  }

  @Override
  public int availableBytes(int offset) {
    return Math.max(source.length - this.offset - offset, 0);
  }

  @Override
  public boolean advance(int advanceBytes) throws IOException {
    offset = Math.min(offset + advanceBytes, source.length);
    return offset < source.length;
  }

  @Override
  public void copyTo(byte[] dest, int destOffset, int len) {
    System.arraycopy(source, offset, dest, destOffset, len);
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
