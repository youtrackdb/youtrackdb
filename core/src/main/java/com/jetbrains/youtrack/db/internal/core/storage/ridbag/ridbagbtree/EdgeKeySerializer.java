package com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

public final class EdgeKeySerializer implements BinarySerializer<EdgeKey> {

  static final EdgeKeySerializer INSTANCE = new EdgeKeySerializer();

  @Override
  public int getObjectSize(EdgeKey object, Object... hints) {
    return LongSerializer.getObjectSize(object.ridBagId)
        + IntSerializer.INSTANCE.getObjectSize(object.targetCluster)
        + LongSerializer.getObjectSize(object.targetPosition);
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return doGetObjectSize(stream, startPosition);
  }

  private int doGetObjectSize(byte[] stream, int startPosition) {
    var size = LongSerializer.getObjectSize(stream, startPosition);
    size += IntSerializer.INSTANCE.getObjectSize(stream, startPosition);
    return size + LongSerializer.getObjectSize(stream, startPosition + size);
  }

  @Override
  public void serialize(EdgeKey object, byte[] stream, int startPosition, Object... hints) {
    doSerialize(object, stream, startPosition);
  }

  private void doSerialize(EdgeKey object, byte[] stream, int startPosition) {
    startPosition = LongSerializer.serialize(object.ridBagId, stream, startPosition);
    startPosition =
        IntSerializer.INSTANCE.serializePrimitive(stream, startPosition, object.targetCluster);
    LongSerializer.serialize(object.targetPosition, stream, startPosition);
  }

  @Override
  public EdgeKey deserialize(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  private EdgeKey doDeserialize(byte[] stream, int startPosition) {
    var ownerId = LongSerializer.deserialize(stream, startPosition);
    var size = LongSerializer.getObjectSize(stream, startPosition);
    startPosition += size;

    final int targetCluster = IntSerializer.INSTANCE.deserialize(stream, startPosition);
    size = IntSerializer.INSTANCE.getObjectSize(stream, startPosition);
    startPosition += size;

    final var targetPosition = LongSerializer.deserialize(stream, startPosition);
    return new EdgeKey(ownerId, targetCluster, targetPosition);
  }

  @Override
  public byte getId() {
    return -1;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return -1;
  }

  @Override
  public void serializeNativeObject(
      EdgeKey object, byte[] stream, int startPosition, Object... hints) {
    doSerialize(object, stream, startPosition);
  }

  @Override
  public EdgeKey deserializeNativeObject(byte[] stream, int startPosition) {
    return doDeserialize(stream, startPosition);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return doGetObjectSize(stream, startPosition);
  }

  @Override
  public EdgeKey preprocess(EdgeKey value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(EdgeKey object, ByteBuffer buffer, Object... hints) {
    LongSerializer.serialize(object.ridBagId, buffer);
    IntSerializer.INSTANCE.serializeInByteBufferObject(object.targetCluster, buffer);
    LongSerializer.serialize(object.targetPosition, buffer);
  }

  @Override
  public EdgeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    final var ownerId = LongSerializer.deserialize(buffer);
    final int targetCluster = IntSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    final var targetPosition = LongSerializer.deserialize(buffer);

    return new EdgeKey(ownerId, targetCluster, targetPosition);
  }

  @Override
  public EdgeKey deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    var delta = LongSerializer.getObjectSize(buffer, offset);
    final var ownerId = LongSerializer.deserialize(buffer, offset);
    offset += delta;

    delta = IntSerializer.INSTANCE.getObjectSizeInByteBuffer(offset, buffer);
    final int targetCluster =
        IntSerializer.INSTANCE.deserializeFromByteBufferObject(offset, buffer);
    offset += delta;

    final var targetPosition = LongSerializer.deserialize(buffer, offset);
    return new EdgeKey(ownerId, targetCluster, targetPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final var position = buffer.position();
    var size = LongSerializer.getObjectSize(buffer);
    buffer.position(position + size);

    size += IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer);
    buffer.position(position + size);

    return size + LongSerializer.getObjectSize(buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    final var position = offset;
    var size = LongSerializer.getObjectSize(buffer, offset);
    offset = position + size;

    size += IntSerializer.INSTANCE.getObjectSizeInByteBuffer(offset, buffer);
    offset = position + size;

    return size + LongSerializer.getObjectSize(buffer, offset);
  }

  @Override
  public EdgeKey deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    var ownerId = LongSerializer.deserialize(buffer, walChanges, offset);
    var size = LongSerializer.getObjectSize(buffer, walChanges, offset);
    offset += size;

    size = IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
    final int targetCluster =
        IntSerializer.INSTANCE.deserializeFromByteBufferObject(buffer, walChanges, offset);
    offset += size;

    final var targetPosition = LongSerializer.deserialize(buffer, walChanges, offset);

    return new EdgeKey(ownerId, targetCluster, targetPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    var size = LongSerializer.getObjectSize(buffer, walChanges, offset);
    size += IntSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset + size);
    return size + LongSerializer.getObjectSize(buffer, walChanges, offset + size);
  }
}
