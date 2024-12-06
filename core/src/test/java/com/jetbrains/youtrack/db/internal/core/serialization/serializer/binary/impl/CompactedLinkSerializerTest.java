package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class CompactedLinkSerializerTest {

  @Test
  public void testSerializeOneByte() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytes() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytes() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeOneByte() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeTwoBytes() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeThreeBytes() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final Identifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteBuffer() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);

    final ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteImmutableBufferPosition() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);

    final ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteBuffer() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteImmutableBufferPosition() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteBuffer() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteImmutableBufferPosition() {
    final CompactedLinkSerializer linkSerializer = new CompactedLinkSerializer();

    final RecordId rid = new RecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final Identifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }
}
