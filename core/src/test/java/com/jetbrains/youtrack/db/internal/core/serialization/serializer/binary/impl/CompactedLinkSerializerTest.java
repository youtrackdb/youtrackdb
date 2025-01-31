package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class CompactedLinkSerializerTest {

  @Test
  public void testSerializeOneByte() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 230);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final var restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytes() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 325);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final var restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytes() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 65628);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final var restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeOneByte() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 230);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final var restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeTwoBytes() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 325);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final var restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeThreeBytes() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 65628);
    final var size = linkSerializer.getObjectSize(rid);
    final var serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final var restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteBuffer() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 230);
    final var size = linkSerializer.getObjectSize(rid);

    final var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteImmutableBufferPosition() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 230);
    final var size = linkSerializer.getObjectSize(rid);

    final var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteBuffer() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 325);
    final var size = linkSerializer.getObjectSize(rid);

    var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteImmutableBufferPosition() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 325);
    final var size = linkSerializer.getObjectSize(rid);

    var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteBuffer() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 65628);
    final var size = linkSerializer.getObjectSize(rid);

    var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteImmutableBufferPosition() {
    final var linkSerializer = new CompactedLinkSerializer();

    final var rid = new RecordId(123, 65628);
    final var size = linkSerializer.getObjectSize(rid);

    var buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final var restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }
}
