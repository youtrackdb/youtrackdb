package com.orientechnologies.common.io;

import java.io.IOException;
import java.io.InputStream;

public class NaiveInputStreamWindow implements BytesSource {

  private final InputStream inputStream;
  private final byte[] buffer;

  private int bufferSize = 0;

  public NaiveInputStreamWindow(InputStream inputStream, int windowSize) throws IOException {
    this.inputStream = inputStream;
    this.buffer = new byte[windowSize];
    advance(windowSize);
  }

  public byte[] get() {
    return buffer;
  }

  public int availableBytes(int offset) {
    return Math.max(bufferSize - offset, 0);
  }

  public int size() {
    return bufferSize;
  }

  public boolean advance(int advanceBytes) throws IOException {

    if (advanceBytes > buffer.length) {
      // this behavior is not needed now
      throw new IOException("Cannot advance more than the buffer size");
    }

    int newSize = 0;
    if (advanceBytes < bufferSize) {
      System.arraycopy(buffer, advanceBytes, buffer, 0, bufferSize - advanceBytes);
      newSize = bufferSize - advanceBytes;
    }

    final int bytesToRead = buffer.length - newSize;

    final int bytesRead = inputStream.read(buffer, newSize, bytesToRead);

    if (bytesRead > 0) {
      newSize += bytesRead;
    }

    bufferSize = Math.max(newSize, 0);
    return bufferSize > 0;
  }

  @Override
  public void copyTo(byte[] dest, int destOffset, int len) {
    System.arraycopy(buffer, 0, dest, destOffset, len);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
