/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jetbrains.youtrack.db.internal.common.serialization.types;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALPageChangesPortion;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 17.01.12
 */
public class IntegerSerializerTest {

  private static final int FIELD_SIZE = 4;
  private static final Integer OBJECT = 1;
  private IntegerSerializer integerSerializer;
  byte[] stream = new byte[FIELD_SIZE];

  @Before
  public void beforeClass() {
    integerSerializer = new IntegerSerializer();
  }

  @Test
  public void testFieldSize() {
    Assert.assertEquals(integerSerializer.getObjectSize(null), FIELD_SIZE);
  }

  @Test
  public void testSerialize() {
    integerSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(integerSerializer.deserialize(stream, 0), OBJECT);
  }

  @Test
  public void testSerializeNative() {
    integerSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(integerSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  @Test
  public void testNativeDirectMemoryCompatibility() {
    integerSerializer.serializeNative(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(integerSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  @Test
  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    integerSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(integerSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(integerSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }

  @Test
  public void testSerializeInImmutableByteBufferPosition() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    integerSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(0);
    Assert.assertEquals(integerSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(
        integerSerializer.deserializeFromByteBufferObject(serializationOffset, buffer), OBJECT);
    Assert.assertEquals(0, buffer.position());
  }

  @Test
  public void testSerializeWALChanges() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(
                FIELD_SIZE + serializationOffset + WALPageChangesPortion.PORTION_BYTES)
            .order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];
    integerSerializer.serializeNative(OBJECT, data, 0);

    WALChanges walChanges = new WALPageChangesPortion();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        integerSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        integerSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT);

    Assert.assertEquals(0, buffer.position());
  }
}
