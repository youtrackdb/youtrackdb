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
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.NullSerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Serializer that is used for serialization of {@link CompositeKey} keys in index.
 *
 * @since 29.07.11
 */
public class CompositeKeySerializer implements BinarySerializer<CompositeKey> {

  public static final CompositeKeySerializer INSTANCE = new CompositeKeySerializer();
  public static final byte ID = 14;

  public int getObjectSize(CompositeKey compositeKey, Object... hints) {
    final PropertyType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();

    int size = 2 * IntegerSerializer.INT_SIZE;

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      if (key != null) {
        final PropertyType type;
        if (types.length > i) {
          type = types[i];
        } else {
          type = PropertyType.getTypeByClass(key.getClass());
        }

        size +=
            BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
                + factory.getObjectSerializer(type).getObjectSize(key);
      } else {
        size +=
            BinarySerializerFactory.TYPE_IDENTIFIER_SIZE
                + NullSerializer.INSTANCE.getObjectSize(null);
      }
    }

    return size;
  }

  public void serialize(
      CompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final PropertyType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += IntegerSerializer.INT_SIZE;

    IntegerSerializer.INSTANCE.serializeLiteral(keysSize, stream, startPosition);

    startPosition += IntegerSerializer.INT_SIZE;

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      BinarySerializer<Object> binarySerializer;
      if (key != null) {
        final PropertyType type;
        if (types.length > i) {
          type = types[i];
        } else {
          type = PropertyType.getTypeByClass(key.getClass());
        }

        binarySerializer = factory.getObjectSerializer(type);
      } else {
        binarySerializer = NullSerializer.INSTANCE;
      }

      stream[startPosition] = binarySerializer.getId();
      startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serialize(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    IntegerSerializer.INSTANCE.serializeLiteral(
        (startPosition - oldStartPosition), stream, oldStartPosition);
  }

  @SuppressWarnings("unchecked")
  public CompositeKey deserialize(byte[] stream, int startPosition) {
    final CompositeKey compositeKey = new CompositeKey();

    startPosition += IntegerSerializer.INT_SIZE;

    final int keysSize = IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    startPosition += IntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      BinarySerializer<Object> binarySerializer =
          (BinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserialize(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  public void serializeNativeObject(
      CompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final PropertyType[] types = getKeyTypes(hints);

    final List<Object> keys = compositeKey.getKeys();
    final int keysSize = keys.size();

    final int oldStartPosition = startPosition;

    startPosition += IntegerSerializer.INT_SIZE;

    IntegerSerializer.INSTANCE.serializeNative(keysSize, stream, startPosition);

    startPosition += IntegerSerializer.INT_SIZE;

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);
      BinarySerializer<Object> binarySerializer;
      if (key != null) {
        final PropertyType type;
        if (types.length > i) {
          type = types[i];
        } else {
          type = PropertyType.getTypeByClass(key.getClass());
        }

        binarySerializer = factory.getObjectSerializer(type);
      } else {
        binarySerializer = NullSerializer.INSTANCE;
      }

      stream[startPosition] = binarySerializer.getId();
      startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      binarySerializer.serializeNativeObject(key, stream, startPosition);
      startPosition += binarySerializer.getObjectSize(key);
    }

    IntegerSerializer.INSTANCE.serializeNative(
        (startPosition - oldStartPosition), stream, oldStartPosition);
  }

  public CompositeKey deserializeNativeObject(byte[] stream, int startPosition) {
    final CompositeKey compositeKey = new CompositeKey();

    startPosition += IntegerSerializer.INT_SIZE;

    final int keysSize = IntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += IntegerSerializer.INSTANCE.getObjectSize(keysSize);

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = stream[startPosition];
      startPosition += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      @SuppressWarnings("unchecked")
      BinarySerializer<Object> binarySerializer =
          (BinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeNativeObject(stream, startPosition);
      compositeKey.addKey(key);

      startPosition += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  private static PropertyType[] getKeyTypes(Object[] hints) {
    final PropertyType[] types;

    if (hints != null && hints.length > 0) {
      types = (PropertyType[]) hints;
    } else {
      types = CommonConst.EMPTY_TYPES_ARRAY;
    }
    return types;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public CompositeKey preprocess(CompositeKey value, Object... hints) {
    if (value == null) {
      return null;
    }

    final PropertyType[] types = getKeyTypes(hints);

    final List<Object> keys = value.getKeys();
    final CompositeKey compositeKey = new CompositeKey();

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keys.size(); i++) {
      Object key = keys.get(i);

      if (key != null) {
        final PropertyType type;
        if (types.length > i) {
          type = types[i];
        } else {
          type = PropertyType.getTypeByClass(key.getClass());
        }

        BinarySerializer<Object> keySerializer = factory.getObjectSerializer(type);
        if (key instanceof Map
            && !(type == PropertyType.EMBEDDEDMAP || type == PropertyType.LINKMAP)
            && ((Map<?, ?>) key).size() == 1
            && ((Map<?, ?>) key)
            .keySet()
            .iterator()
            .next()
            .getClass()
            .isAssignableFrom(type.getDefaultJavaType())) {
          key = ((Map<?, ?>) key).keySet().iterator().next();
        }
        compositeKey.addKey(keySerializer.preprocess(key));
      } else {
        compositeKey.addKey(key);
      }
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serializeInByteBufferObject(
      CompositeKey object, ByteBuffer buffer, Object... hints) {
    final PropertyType[] types = getKeyTypes(hints);

    final List<Object> keys = object.getKeys();
    final int keysSize = keys.size();

    final int oldStartOffset = buffer.position();
    buffer.position(oldStartOffset + IntegerSerializer.INT_SIZE);

    buffer.putInt(keysSize);
    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);

      BinarySerializer<Object> binarySerializer;
      if (key != null) {
        final PropertyType type;
        if (types.length > i) {
          type = types[i];
        } else {
          type = PropertyType.getTypeByClass(key.getClass());
        }

        binarySerializer = factory.getObjectSerializer(type);
      } else {
        binarySerializer = NullSerializer.INSTANCE;
      }

      buffer.put(binarySerializer.getId());
      binarySerializer.serializeInByteBufferObject(key, buffer);
    }

    final int finalPosition = buffer.position();
    final int serializedSize = buffer.position() - oldStartOffset;

    buffer.position(oldStartOffset);
    buffer.putInt(serializedSize);

    buffer.position(finalPosition);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompositeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    final CompositeKey compositeKey = new CompositeKey();

    buffer.position(buffer.position() + IntegerSerializer.INT_SIZE);
    final int keysSize = buffer.getInt();

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = buffer.get();
      @SuppressWarnings("unchecked")
      BinarySerializer<Object> binarySerializer =
          (BinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key = binarySerializer.deserializeFromByteBufferObject(buffer);
      compositeKey.addKey(key);
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompositeKey deserializeFromByteBufferObject(int offset, ByteBuffer buffer) {
    final CompositeKey compositeKey = new CompositeKey();

    offset += IntegerSerializer.INT_SIZE;
    final int keysSize = buffer.getInt(offset);
    offset += IntegerSerializer.INT_SIZE;

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = buffer.get(offset);
      offset++;
      @SuppressWarnings("unchecked")
      BinarySerializer<Object> binarySerializer =
          (BinarySerializer<Object>) factory.getObjectSerializer(serializerId);

      var delta = binarySerializer.getObjectSizeInByteBuffer(offset, buffer);
      final Object key = binarySerializer.deserializeFromByteBufferObject(offset, buffer);
      offset += delta;

      compositeKey.addKey(key);
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  @Override
  public int getObjectSizeInByteBuffer(int offset, ByteBuffer buffer) {
    return buffer.getInt(offset);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompositeKey deserializeFromByteBufferObject(
      ByteBuffer buffer, WALChanges walChanges, int offset) {
    final CompositeKey compositeKey = new CompositeKey();

    offset += IntegerSerializer.INT_SIZE;

    final int keysSize = walChanges.getIntValue(buffer, offset);
    offset += IntegerSerializer.INT_SIZE;

    final BinarySerializerFactory factory = BinarySerializerFactory.getInstance();
    for (int i = 0; i < keysSize; i++) {
      final byte serializerId = walChanges.getByteValue(buffer, offset);
      offset += BinarySerializerFactory.TYPE_IDENTIFIER_SIZE;

      @SuppressWarnings("unchecked")
      BinarySerializer<Object> binarySerializer =
          (BinarySerializer<Object>) factory.getObjectSerializer(serializerId);
      final Object key =
          binarySerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
      compositeKey.addKey(key);

      offset += binarySerializer.getObjectSize(key);
    }

    return compositeKey;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, WALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}
