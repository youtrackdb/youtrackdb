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

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
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

  public int getObjectSize(BinarySerializerFactory serializerFactory, T key, Object... hints) {
    init(key, hints, serializerFactory);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE + binarySerializer.getObjectSize(
        serializerFactory, key);
  }

  public void serialize(T key, BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition, Object... hints) {
    init(key, hints, serializerFactory);
    stream[startPosition] = binarySerializer.getId();
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serialize(key, serializerFactory, stream, startPosition);
  }

  public T deserialize(BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition) {
    final var typeId = stream[startPosition];
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId, serializerFactory);
    return (T) binarySerializer.deserialize(serializerFactory, stream, startPosition);
  }

  public int getObjectSize(BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition) {
    final var serializerId = stream[startPosition];
    init(serializerId, serializerFactory);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSize(serializerFactory,
        stream, startPosition + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public byte getId() {
    return ID;
  }

  protected void init(T key, Object[] hints, BinarySerializerFactory serializerFactory) {
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

      binarySerializer = serializerFactory.getObjectSerializer(type);
    }
  }

  protected void init(byte serializerId, BinarySerializerFactory serializerFactory) {
    if (binarySerializer == null) {
      binarySerializer = serializerFactory.getObjectSerializer(serializerId);
    }
  }

  public int getObjectSizeNative(BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition) {
    final var serializerId = stream[startPosition];
    init(serializerId, serializerFactory);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeNative(
        serializerFactory, stream, startPosition + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE);
  }

  public void serializeNativeObject(T key, BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition, Object... hints) {
    init(key, hints, serializerFactory);
    stream[startPosition] = binarySerializer.getId();
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
    binarySerializer.serializeNativeObject(key, serializerFactory, stream, startPosition);
  }

  public T deserializeNativeObject(BinarySerializerFactory serializerFactory, byte[] stream,
      int startPosition) {
    final var typeId = stream[startPosition];
    startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

    init(typeId, serializerFactory);
    return (T) binarySerializer.deserializeNativeObject(serializerFactory, stream, startPosition);
  }

  public boolean isFixedLength() {
    return binarySerializer.isFixedLength();
  }

  public int getFixedLength() {
    return binarySerializer.getFixedLength() + BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;
  }

  @Override
  public T preprocess(BinarySerializerFactory serializerFactory, T value, Object... hints) {
    init(value, hints, serializerFactory);

    return (T) binarySerializer.preprocess(serializerFactory, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(BinarySerializerFactory serializerFactory, T object,
      ByteBuffer buffer, Object... hints) {
    init(object, hints, serializerFactory);
    buffer.put(binarySerializer.getId());
    binarySerializer.serializeInByteBufferObject(serializerFactory, object, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T deserializeFromByteBufferObject(BinarySerializerFactory serializerFactory,
      ByteBuffer buffer) {
    final var typeId = buffer.get();

    init(typeId, serializerFactory);
    return (T) binarySerializer.deserializeFromByteBufferObject(serializerFactory, buffer);
  }

  @Override
  public T deserializeFromByteBufferObject(BinarySerializerFactory serializerFactory, int offset,
      ByteBuffer buffer) {
    final var typeId = buffer.get(offset);
    offset++;

    init(typeId, serializerFactory);
    return (T) binarySerializer.deserializeFromByteBufferObject(serializerFactory, offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(BinarySerializerFactory serializerFactory,
      ByteBuffer buffer) {
    final var serializerId = buffer.get();
    init(serializerId, serializerFactory);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(serializerFactory, buffer);
  }

  @Override
  public int getObjectSizeInByteBuffer(BinarySerializerFactory serializerFactory, int offset,
      ByteBuffer buffer) {
    final var serializerId = buffer.get(offset);
    offset++;

    init(serializerId, serializerFactory);
    return BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
        + binarySerializer.getObjectSizeInByteBuffer(serializerFactory, offset, buffer);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T deserializeFromByteBufferObject(BinarySerializerFactory serializerFactory,
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final var typeId = walChanges.getByteValue(buffer, offset++);

    init(typeId, serializerFactory);
    return (T) binarySerializer.deserializeFromByteBufferObject(serializerFactory, buffer,
        walChanges, offset);
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
