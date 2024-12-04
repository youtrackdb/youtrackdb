package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRecordId;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

public class CompactedLinkSerializerTest {

  @Test
  public void testSerializeOneByte() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytes() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytes() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serialize(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSize(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserialize(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeOneByte() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeTwoBytes() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeNativeThreeBytes() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);
    final byte[] serialized = new byte[size + 1];
    linkSerializer.serializeNativeObject(rid, serialized, 1);

    Assert.assertEquals(size, linkSerializer.getObjectSizeNative(serialized, 1));

    final YTIdentifiable restoredRid = linkSerializer.deserializeNativeObject(serialized, 1);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteBuffer() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);

    final ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeOneByteByteImmutableBufferPosition() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 230);
    final int size = linkSerializer.getObjectSize(rid);

    final ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteBuffer() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeTwoBytesByteImmutableBufferPosition() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 325);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteBuffer() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(1);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(buffer));

    buffer.position(1);
    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(buffer);
    Assert.assertEquals(rid, restoredRid);
  }

  @Test
  public void testSerializeThreeBytesInByteImmutableBufferPosition() {
    final OCompactedLinkSerializer linkSerializer = new OCompactedLinkSerializer();

    final YTRecordId rid = new YTRecordId(123, 65628);
    final int size = linkSerializer.getObjectSize(rid);

    ByteBuffer buffer = ByteBuffer.allocate(size + 1);
    buffer.position(1);
    linkSerializer.serializeInByteBufferObject(rid, buffer);

    buffer.position(0);
    Assert.assertEquals(size, linkSerializer.getObjectSizeInByteBuffer(1, buffer));
    Assert.assertEquals(0, buffer.position());

    final YTIdentifiable restoredRid = linkSerializer.deserializeFromByteBufferObject(1, buffer);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(rid, restoredRid);
  }
}
