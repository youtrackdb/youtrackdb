package com.jetbrains.youtrack.db.internal.common.serialization.types;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UTF8Serializer implements BinarySerializer<String> {

  private static final int INT_MASK = 0xFFFF;

  public static final UTF8Serializer INSTANCE = new UTF8Serializer();
  public static final byte ID = 25;

  @Override
  public int getObjectSize(String object, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    return ShortSerializer.SHORT_SIZE + encoded.length;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return (ShortSerializer.INSTANCE.deserialize(stream, startPosition) & INT_MASK)
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serialize(String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    ShortSerializer.INSTANCE.serialize((short) encoded.length, stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserialize(byte[] stream, int startPosition) {
    final int encodedSize = ShortSerializer.INSTANCE.deserialize(stream, startPosition) & INT_MASK;
    startPosition += ShortSerializer.SHORT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return 0;
  }

  @Override
  public void serializeNativeObject(
      String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    ShortSerializer.INSTANCE.serializeNative((short) encoded.length, stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserializeNativeObject(byte[] stream, int startPosition) {
    final int encodedSize =
        ShortSerializer.INSTANCE.deserializeNative(stream, startPosition) & INT_MASK;
    startPosition += ShortSerializer.SHORT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return (ShortSerializer.INSTANCE.deserializeNative(stream, startPosition) & INT_MASK)
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public String preprocess(String value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(String object, ByteBuffer buffer, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    buffer.putShort((short) encoded.length);

    buffer.put(encoded);
  }

  @Override
  public String deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int encodedSize = buffer.getShort() & INT_MASK;

    final byte[] encoded = new byte[encodedSize];
    buffer.get(encoded);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public String deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final int encodedSize = buffer.getShort(offset) & INT_MASK;
    offset += Short.BYTES;

    final byte[] encoded = new byte[encodedSize];
    buffer.get(offset, encoded);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return (buffer.getShort() & INT_MASK) + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return (buffer.getShort(offset) & INT_MASK) + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public String deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final int encodedSize = walChanges.getShortValue(buffer, offset) & INT_MASK;
    offset += ShortSerializer.SHORT_SIZE;

    final byte[] encoded = walChanges.getBinaryValue(buffer, offset, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return (walChanges.getShortValue(buffer, offset) & INT_MASK) + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public byte[] serializeNativeAsWhole(String object, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    final byte[] result = new byte[encoded.length + ShortSerializer.SHORT_SIZE];

    ShortSerializer.INSTANCE.serializeNative((short) encoded.length, result, 0);
    System.arraycopy(encoded, 0, result, ShortSerializer.SHORT_SIZE, encoded.length);
    return result;
  }
}
