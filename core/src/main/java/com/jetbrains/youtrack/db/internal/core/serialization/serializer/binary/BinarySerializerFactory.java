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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinaryTypeSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BooleanSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.CharSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DateSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DateTimeSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DecimalSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DoubleSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.FloatSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.NullSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.StringSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UTF8Serializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.CompactedLinkSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index.SimpleKeySerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.StreamSerializerRID;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.multivalue.v2.MultiValueEntrySerializer;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectArrayMap;
import java.util.EnumMap;
import javax.annotation.Nonnull;

/**
 * This class is responsible for obtaining OBinarySerializer realization, by it's id of type of
 * object that should be serialized.
 */
public class BinarySerializerFactory {

  /**
   * Size of the type identifier block size
   */
  public static final int TYPE_IDENTIFIER_SIZE = 1;

  public static final byte CURRENT_BINARY_FORMAT_VERSION = 0;

  private final Byte2ObjectArrayMap<BinarySerializer<?>> serializerIdMap = new Byte2ObjectArrayMap<>();
  private final Byte2ObjectArrayMap<Class<? extends BinarySerializer>> serializerClassesIdMap =
      new Byte2ObjectArrayMap<>();
  private final EnumMap<PropertyType, BinarySerializer<?>> serializerTypeMap = new EnumMap<>(
      PropertyType.class);


  private BinarySerializerFactory() {
  }

  public static byte currentBinaryFormatVersion() {
    return CURRENT_BINARY_FORMAT_VERSION;
  }

  public static BinarySerializerFactory create(int binaryFormatVersion) {
    if (binaryFormatVersion != CURRENT_BINARY_FORMAT_VERSION) {
      throw new StorageException(null,
          "Binary format version " + binaryFormatVersion + " is not supported");
    }

    final var factory = new BinarySerializerFactory();

    // STATELESS SERIALIER
    factory.registerSerializer(new NullSerializer(), null);

    factory.registerSerializer(BooleanSerializer.INSTANCE, PropertyType.BOOLEAN);
    factory.registerSerializer(IntegerSerializer.INSTANCE, PropertyType.INTEGER);
    factory.registerSerializer(ShortSerializer.INSTANCE, PropertyType.SHORT);
    factory.registerSerializer(LongSerializer.INSTANCE, PropertyType.LONG);
    factory.registerSerializer(FloatSerializer.INSTANCE, PropertyType.FLOAT);
    factory.registerSerializer(DoubleSerializer.INSTANCE, PropertyType.DOUBLE);
    factory.registerSerializer(DateTimeSerializer.INSTANCE, PropertyType.DATETIME);
    factory.registerSerializer(CharSerializer.INSTANCE, null);
    factory.registerSerializer(StringSerializer.INSTANCE, PropertyType.STRING);

    factory.registerSerializer(ByteSerializer.INSTANCE, PropertyType.BYTE);
    factory.registerSerializer(DateSerializer.INSTANCE, PropertyType.DATE);
    factory.registerSerializer(LinkSerializer.INSTANCE, PropertyType.LINK);
    factory.registerSerializer(CompositeKeySerializer.INSTANCE, null);
    factory.registerSerializer(StreamSerializerRID.INSTANCE, null);
    factory.registerSerializer(BinaryTypeSerializer.INSTANCE, PropertyType.BINARY);
    factory.registerSerializer(DecimalSerializer.INSTANCE, PropertyType.DECIMAL);

    // STATEFUL SERIALIER
    factory.registerSerializer(SimpleKeySerializer.ID, SimpleKeySerializer.class);

    factory.registerSerializer(CompactedLinkSerializer.INSTANCE, null);
    factory.registerSerializer(UTF8Serializer.INSTANCE, null);
    factory.registerSerializer(MultiValueEntrySerializer.INSTANCE, null);

    //used for spatial indexes
    factory.registerSerializer(MockSerializer.INSTANCE, PropertyType.EMBEDDED);
    return factory;
  }

  public static BinarySerializerFactory getInstance(@Nonnull DatabaseSessionInternal session) {
    return session.getSerializerFactory();
  }

  public void registerSerializer(final BinarySerializer<?> iInstance, final PropertyType iType) {
    if (serializerIdMap.containsKey(iInstance.getId())) {
      throw new IllegalArgumentException(
          "Binary serializer with id " + iInstance.getId() + " has been already registered.");
    }

    serializerIdMap.put(iInstance.getId(), iInstance);
    if (iType != null) {
      serializerTypeMap.put(iType, iInstance);
    }
  }

  @SuppressWarnings({"rawtypes"})
  public void registerSerializer(final byte iId,
      final Class<? extends BinarySerializer> iClass) {
    if (serializerClassesIdMap.containsKey(iId)) {
      throw new IllegalStateException(
          "Serializer with id " + iId + " has been already registered.");
    }

    serializerClassesIdMap.put(iId, iClass);
  }

  /**
   * Obtain OBinarySerializer instance by it's id.
   *
   * @param identifier is serializes identifier.
   * @return OBinarySerializer instance.
   */
  public BinarySerializer<?> getObjectSerializer(final byte identifier) {
    var impl = serializerIdMap.get(identifier);
    if (impl == null) {
      final var cls = serializerClassesIdMap.get(identifier);
      if (cls != null) {
        try {
          impl = cls.newInstance();
        } catch (Exception e) {
          LogManager.instance()
              .error(
                  this,
                  "Cannot create an instance of class %s invoking the empty constructor",
                  e,
                  cls);
        }
      }
    }
    return impl;
  }

  /**
   * Obtain OBinarySerializer realization for the PropertyType
   *
   * @param type is the PropertyType to obtain serializer algorithm for
   * @return OBinarySerializer instance
   */
  @SuppressWarnings("unchecked")
  public <T> BinarySerializer<T> getObjectSerializer(final PropertyType type) {
    return (BinarySerializer<T>) serializerTypeMap.get(type);
  }
}
