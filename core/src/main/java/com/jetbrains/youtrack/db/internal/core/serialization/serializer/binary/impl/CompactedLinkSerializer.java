package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl;

import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.bytes2short;
import static com.jetbrains.youtrack.db.internal.core.serialization.BinaryProtocol.short2bytes;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

public class CompactedLinkSerializer implements BinarySerializer<Identifiable> {

  public static final byte ID = 22;
  public static final CompactedLinkSerializer INSTANCE = new CompactedLinkSerializer();

  @Override
  public int getObjectSize(Identifiable rid, Object... hints) {
    final RID r = rid.getIdentity();

    int size = ShortSerializer.SHORT_SIZE + ByteSerializer.BYTE_SIZE;

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;
    size += numberSize;

    return size;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return stream[startPosition + ShortSerializer.SHORT_SIZE]
        + ByteSerializer.BYTE_SIZE
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serialize(Identifiable rid, byte[] stream, int startPosition, Object... hints) {
    final RID r = rid.getIdentity();

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    short2bytes((short) r.getClusterId(), stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    stream[startPosition] = (byte) numberSize;
    startPosition++;

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      stream[startPosition + i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }
  }

  @Override
  public Identifiable deserialize(byte[] stream, int startPosition) {
    final int cluster = bytes2short(stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    final int numberSize = stream[startPosition];
    startPosition++;

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((long) (0xFF & stream[startPosition + i]) << (i * 8));
    }

    return new RecordId(cluster, position);
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
      Identifiable rid, byte[] stream, int startPosition, Object... hints) {
    final RID r = rid.getIdentity();

    ShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    stream[startPosition] = (byte) numberSize;
    startPosition++;

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      stream[startPosition + i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }
  }

  @Override
  public Identifiable deserializeNativeObject(byte[] stream, int startPosition) {
    final int cluster = ShortSerializer.INSTANCE.deserializeNativeObject(stream, startPosition);
    startPosition += ShortSerializer.SHORT_SIZE;

    final int numberSize = stream[startPosition];
    startPosition++;

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((long) (0xFF & stream[startPosition + i]) << (i * 8));
    }

    return new RecordId(cluster, position);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return stream[startPosition + ShortSerializer.SHORT_SIZE]
        + ByteSerializer.BYTE_SIZE
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public Identifiable preprocess(Identifiable value, Object... hints) {
    return value.getIdentity();
  }

  @Override
  public void serializeInByteBufferObject(Identifiable rid, ByteBuffer buffer, Object... hints) {
    final RID r = rid.getIdentity();
    buffer.putShort((short) r.getClusterId());

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    buffer.put((byte) numberSize);

    final byte[] number = new byte[numberSize];

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      number[i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }

    buffer.put(number);
  }

  @Override
  public Identifiable deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int cluster = buffer.getShort();

    final int numberSize = buffer.get();
    final byte[] number = new byte[numberSize];
    buffer.get(number);

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((long) (0xFF & number[i]) << (i * 8));
    }

    return new RecordId(cluster, position);
  }

  @Override
  public Identifiable deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final int cluster = buffer.getShort(offset);
    offset += Short.BYTES;

    final int numberSize = buffer.get(offset);
    offset += Byte.BYTES;

    final byte[] number = new byte[numberSize];
    buffer.get(offset, number);

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((long) (0xFF & number[i]) << (i * 8));
    }

    return new RecordId(cluster, position);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.get(buffer.position() + ShortSerializer.SHORT_SIZE)
        + ByteSerializer.BYTE_SIZE
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return buffer.get(offset + ShortSerializer.SHORT_SIZE)
        + ByteSerializer.BYTE_SIZE
        + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public Identifiable deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final int cluster = walChanges.getShortValue(buffer, offset);
    offset += ShortSerializer.SHORT_SIZE;

    final int numberSize = walChanges.getByteValue(buffer, offset);
    offset++;

    final byte[] number = walChanges.getBinaryValue(buffer, offset, numberSize);

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((long) (0xFF & number[i]) << (i * 8));
    }

    return new RecordId(cluster, position);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return walChanges.getByteValue(buffer, offset + ShortSerializer.SHORT_SIZE)
        + ByteSerializer.BYTE_SIZE
        + ShortSerializer.SHORT_SIZE;
  }
}
