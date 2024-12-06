package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.multivalue.v2;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

public final class MultiValueEntrySerializer implements BinarySerializer<MultiValueEntry> {

  public static final int ID = 27;
  public static final MultiValueEntrySerializer INSTANCE = new MultiValueEntrySerializer();

  @Override
  public int getObjectSize(final MultiValueEntry object, final Object... hints) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public int getObjectSize(final byte[] stream, final int startPosition) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serialize(
      final MultiValueEntry object,
      final byte[] stream,
      final int startPosition,
      final Object... hints) {
    int pos = startPosition;
    LongSerializer.INSTANCE.serialize(object.id, stream, pos);
    pos += LongSerializer.LONG_SIZE;

    ShortSerializer.INSTANCE.serialize((short) object.clusterId, stream, pos);
    pos += ShortSerializer.SHORT_SIZE;

    LongSerializer.INSTANCE.serialize(object.clusterPosition, stream, pos);
  }

  @Override
  public MultiValueEntry deserialize(final byte[] stream, final int startPosition) {
    int pos = startPosition;
    final long id = LongSerializer.INSTANCE.deserialize(stream, pos);
    pos += LongSerializer.LONG_SIZE;

    final int clusterId = ShortSerializer.INSTANCE.deserialize(stream, pos);
    pos += ShortSerializer.SHORT_SIZE;

    final long clusterPosition = LongSerializer.INSTANCE.deserialize(stream, pos);
    return new MultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return true;
  }

  @Override
  public int getFixedLength() {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serializeNativeObject(
      final MultiValueEntry object,
      final byte[] stream,
      final int startPosition,
      final Object... hints) {
    int pos = startPosition;
    LongSerializer.INSTANCE.serializeNative(object.id, stream, pos);
    pos += LongSerializer.LONG_SIZE;

    ShortSerializer.INSTANCE.serializeNative((short) object.clusterId, stream, pos);
    pos += ShortSerializer.SHORT_SIZE;

    LongSerializer.INSTANCE.serializeNative(object.clusterPosition, stream, pos);
  }

  @Override
  public MultiValueEntry deserializeNativeObject(final byte[] stream, final int startPosition) {
    int pos = startPosition;
    final long id = LongSerializer.INSTANCE.deserializeNative(stream, pos);
    pos += LongSerializer.LONG_SIZE;

    final int clusterId = ShortSerializer.INSTANCE.deserializeNative(stream, pos);
    pos += ShortSerializer.SHORT_SIZE;

    final long clusterPosition = LongSerializer.INSTANCE.deserializeNative(stream, pos);
    return new MultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public MultiValueEntry preprocess(final MultiValueEntry value, final Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(
      final MultiValueEntry object, final ByteBuffer buffer, final Object... hints) {
    buffer.putLong(object.id);
    buffer.putShort((short) object.clusterId);
    buffer.putLong(object.clusterPosition);
  }

  @Override
  public MultiValueEntry deserializeFromByteBufferObject(final ByteBuffer buffer) {
    final long id = buffer.getLong();
    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    return new MultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public MultiValueEntry deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final long id = buffer.getLong(offset);
    offset += Long.BYTES;

    final int clusterId = buffer.getShort(offset);
    offset += Short.BYTES;

    final long clusterPosition = buffer.getLong(offset);

    return new MultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(final ByteBuffer buffer) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }

  @Override
  public MultiValueEntry deserializeFromByteBufferObject(
      final ByteBuffer buffer, final WALChanges walChanges, final int offset) {
    int position = offset;

    final long id = walChanges.getLongValue(buffer, position);
    position += LongSerializer.LONG_SIZE;

    final int clusterId = walChanges.getShortValue(buffer, position);
    position += ShortSerializer.SHORT_SIZE;

    final long clusterPosition = walChanges.getLongValue(buffer, position);

    return new MultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(
      final ByteBuffer buffer, final WALChanges walChanges, final int offset) {
    return 2 * LongSerializer.LONG_SIZE + ShortSerializer.SHORT_SIZE;
  }
}
