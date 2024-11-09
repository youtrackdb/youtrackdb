package com.orientechnologies.common.io;

import java.io.IOException;
import java.io.InputStream;

public class CircularInputStreamWindow implements BytesSource {

  private final InputStream inputStream;
  private final byte[] buffer;

  private int bufferSize = 0;
  private int bufferOffset = 0;

  public CircularInputStreamWindow(InputStream inputStream, int windowSize) throws IOException {
    this.inputStream = inputStream;
    this.buffer = new byte[windowSize];
    advance(windowSize);
  }

  @Override
  public int availableBytes(int offset) {
    return Math.max(bufferSize - offset, 0);
  }

  @Override
  public boolean advance(int advanceBytes) throws IOException {

    if (advanceBytes > buffer.length) {
      // this behavior is not needed now
      throw new IOException("Cannot advance more than the buffer size");
    }

    // new offset after advancing
    bufferOffset = normOffset(bufferOffset + advanceBytes);

    // how maybe bytes of the buffer are left after advancing
    final int bytesLeft = Math.max(bufferSize - advanceBytes, 0);

    // how many bytes are left to read from the source stream
    final int bytesToRead = buffer.length - bytesLeft;

    // position in the buffer at which to start writing
    final int writeOffset = normOffset(bufferOffset + bytesLeft);

    final int chunk1Size = Math.min(bytesToRead, buffer.length - writeOffset);
    final int chunk2Size = bytesToRead - chunk1Size;

    final int read1 = inputStream.read(buffer, writeOffset, chunk1Size);

    final int read2 = inputStream.read(buffer, 0, chunk2Size);

    bufferSize = bytesLeft + Math.max(read1, 0) + Math.max(read2, 0);

    return bufferSize > 0;
  }

  private int normOffset(int offset) {
    return offset % buffer.length;
  }

  @Override
  public void copyTo(byte[] dest, int destOffset, int len) {

    final int chunk1Size = Math.min(len, buffer.length - bufferOffset);
    final int chunk2Size = len - chunk1Size;

    System.arraycopy(buffer, bufferOffset, dest, destOffset, chunk1Size);
    System.arraycopy(buffer, 0, dest, destOffset + chunk1Size, chunk2Size);
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}
