/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;

/**
 * Serializer that is used for serialization of non {@link CompositeKey} keys in index.
 *
 * @since 31.03.12
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SimpleKeySerializer<T extends Comparable<?>> implements BinarySerializer<T> {

  private BinarySerializer binarySerializer;

  public static final byte ID = 15;
  public static final String NAME = "bsks";

  public SimpleKeySerializer() {
  }

  public int getObjectSize(T key, Object... hints) {
    init(key, hints);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE + binarySerializer.getObjectSize(key);
  }

  public void serialize(T key, byte[] stream, int startPosition, Object... hints) {
    init(key, hints);
    stream[startPosition] = binarySerializer.getId();
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serialize(key, stream, startPosition);
  }

  public T deserialize(byte[] stream, int startPosition) {
    final byte typeId = stream[startPosition];
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId);
    return (T) binarySerializer.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    final byte serializerId = stream[startPosition];
    init(serializerId);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSize(
        stream, startPosition + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public byte getId() {
    return ID;
  }

  protected void init(T key, Object[] hints) {
    if (binarySerializer == null) {
      final PropertyType[] types;

      if (hints != null && hints.length > 0) {
        types = (PropertyType[]) hints;
      } else {
        types = CommonConst.EMPTY_TYPES_ARRAY;
      }

      PropertyType type;
      if (types.length > 0) {
        type = types[0];
      } else {
        type = PropertyType.getTypeByClass(key.getClass());
      }

      binarySerializer = BinarySerializerFactory.getInstance().getObjectSerializer(type);
    }
  }

  protected void init(byte serializerId) {
    if (binarySerializer == null) {
      binarySerializer = BinarySerializerFactory.getInstance().getObjectSerializer(serializerId);
    }
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    final byte serializerId = stream[startPosition];
    init(serializerId);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeNative(
        stream, startPosition + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public void serializeNativeObject(T key, byte[] stream, int startPosition, Object... hints) {
    init(key, hints);
    stream[startPosition] = binarySerializer.getId();
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serializeNativeObject(key, stream, startPosition);
  }

  public T deserializeNativeObject(byte[] stream, int startPosition) {
    final byte typeId = stream[startPosition];
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId);
    return (T) binarySerializer.deserializeNativeObject(stream, startPosition);
  }

  public boolean isFixedLength() {
    return binarySerializer.isFixedLength();
  }

  public int getFixedLength() {
    return binarySerializer.getFixedLength() + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
  }

  @Override
  public T preprocess(T value, Object... hints) {
    init(value, hints);

    return (T) binarySerializer.preprocess(value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(T object, ByteBuffer buffer, Object... hints) {
    init(object, hints);
    buffer.put(binarySerializer.getId());
    binarySerializer.serializeInByteBufferObject(object, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T deserializeFromByteBufferObject(ByteBuffer buffer) {
    final byte typeId = buffer.get();

    init(typeId);
    return (T) binarySerializer.deserializeFromByteBufferObject(buffer);
  }

  @Override
  public T deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final byte typeId = buffer.get(offset);
    offset++;

    init(typeId);
    return (T) binarySerializer.deserializeFromByteBufferObject(offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final byte serializerId = buffer.get();
    init(serializerId);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    final byte serializerId = buffer.get(offset);
    offset++;

    init(serializerId);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T deserializeFromByteBufferObject(ByteBuffer buffer, WALChanges walChanges, int offset) {
    final byte typeId = walChanges.getByteValue(buffer, offset++);

    init(typeId);
    return (T) binarySerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(
        buffer, walChanges, BinarySerializerFactory.TYPE_IDENTIFIER_SIZE + offset);
  }
}
