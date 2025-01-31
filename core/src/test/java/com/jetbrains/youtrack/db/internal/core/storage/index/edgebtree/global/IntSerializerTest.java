package com.jetbrains.youtrack.db.internal.core.storage.index.edgebtree.global;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALPageChangesPortion;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.IntSerializer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Test;

public class IntSerializerTest {

  @Test
  public void serializeOneByteTest() {
    final var value = 0xE5;

    serializationTest(value);
  }

  @Test
  public void serializeTwoBytesTest() {
    final var value = 0xE4_E5;

    serializationTest(value);
  }

  @Test
  public void serializeThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    serializationTest(value);
  }

  @Test
  public void serializeFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    serializationTest(value);
  }

  private void serializationTest(int value) {
    final var serializer = new IntSerializer();

    final var size = serializer.getObjectSize(value);
    final var stream = new byte[size + 3];

    serializer.serialize(value, stream, 3, (Object[]) null);

    final var serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserialize(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializePrimitiveOneByteTest() {
    final var value = 0xE5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveTwoBytesTest() {
    final var value = 0xE4_E5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    primitiveSerializationTest(value);
  }

  private void primitiveSerializationTest(int value) {
    final var serializer = new IntSerializer();

    final var size = serializer.getObjectSize(value);
    final var stream = new byte[size + 3];

    final var position = serializer.serializePrimitive(stream, 3, value);
    Assert.assertEquals(size + 3, position);

    final var serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserialize(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeNativeOneByteTest() {
    final var value = 0xE5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeTwoBytesTest() {
    final var value = 0xE4_E5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    nativeSerializationTest(value);
  }

  private void nativeSerializationTest(int value) {
    final var serializer = new IntSerializer();

    final var size = serializer.getObjectSize(value);
    final var stream = new byte[size + 3];

    serializer.serializeNativeObject(value, stream, 3, (Object[]) null);

    final var serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserializeNativeObject(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeByteBufferOneByteTest() {
    final var value = 0xE5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeImmutableByteBufferPositionOneByteTest() {
    final var value = 0xE5;

    byteBufferImmutablePositionSerializationTest(value);
  }

  @Test
  public void serializeByteBufferTwoBytesTest() {
    final var value = 0xE4_E5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeImmutableByteBufferPositionTwoBytesTest() {
    final var value = 0xE4_E5;

    byteBufferImmutablePositionSerializationTest(value);
  }

  @Test
  public void serializeByteBufferThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeImmutableByteBufferPositionThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    byteBufferImmutablePositionSerializationTest(value);
  }

  @Test
  public void serializeByteBufferFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeImmutableByteBufferPositionFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    byteBufferImmutablePositionSerializationTest(value);
  }

  private void byteBufferSerializationTest(int value) {
    final var serializer = new IntSerializer();

    final var size = serializer.getObjectSize(value);
    final var byteBuffer = ByteBuffer.allocate(size + 3);

    byteBuffer.position(3);

    serializer.serializeInByteBufferObject(value, byteBuffer, (Object[]) null);
    Assert.assertEquals(size + 3, byteBuffer.position());

    byteBuffer.position(3);
    final var serializedSize = serializer.getObjectSizeInByteBuffer(byteBuffer);
    Assert.assertEquals(size, serializedSize);

    byteBuffer.position(3);
    final int deserialized = serializer.deserializeFromByteBufferObject(byteBuffer);

    Assert.assertEquals(value, deserialized);
  }

  private void byteBufferImmutablePositionSerializationTest(int value) {
    final var serializer = new IntSerializer();

    final var size = serializer.getObjectSize(value);
    final var byteBuffer = ByteBuffer.allocate(size + 3);

    byteBuffer.position(3);

    serializer.serializeInByteBufferObject(value, byteBuffer, (Object[]) null);
    Assert.assertEquals(size + 3, byteBuffer.position());

    byteBuffer.position(0);
    final var serializedSize = serializer.getObjectSizeInByteBuffer(3, byteBuffer);
    Assert.assertEquals(size, serializedSize);
    Assert.assertEquals(0, byteBuffer.position());

    final int deserialized = serializer.deserializeFromByteBufferObject(3, byteBuffer);
    Assert.assertEquals(0, byteBuffer.position());

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeChangesOneByteTest() {
    final var value = 0xE5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeChangesTwoBytesTest() {
    final var value = 0xE4_E5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeChangesThreeBytesTest() {
    final var value = 0xA5_E4_E5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeByteChangesFourBytesTest() {
    final var value = 0xFE_A5_E4_E5;

    changeTrackingSerializationTest(value);
  }

  private void changeTrackingSerializationTest(int value) {
    final var serializer = new IntSerializer();
    final WALChanges walChanges = new WALPageChangesPortion();

    final var size = serializer.getObjectSize(value);
    final var byteBuffer =
        ByteBuffer.allocate(GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024)
            .order(ByteOrder.nativeOrder());
    final var serializedValue = new byte[size];
    serializer.serializeNativeObject(value, serializedValue, 0);
    walChanges.setBinaryValue(byteBuffer, serializedValue, 3);

    final var serializedSize = serializer.getObjectSizeInByteBuffer(byteBuffer, walChanges, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserializeFromByteBufferObject(byteBuffer, walChanges, 3);

    Assert.assertEquals(value, deserialized);

    Assert.assertEquals(0, byteBuffer.position());
  }
}
