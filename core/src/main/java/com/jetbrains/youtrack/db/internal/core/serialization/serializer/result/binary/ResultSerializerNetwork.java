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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.result.binary;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DecimalSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.SerializableStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.BytesContainer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.SerializableWrapper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.VarIntSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

public class ResultSerializerNetwork {

  private static final String CHARSET_UTF_8 = "UTF-8";
  private static final RecordId NULL_RECORD_ID = new RecordId(-2, RID.CLUSTER_POS_INVALID);
  private static final long MILLISEC_PER_DAY = 86400000;

  public ResultSerializerNetwork() {
  }

  public ResultInternal deserialize(DatabaseSessionInternal db, final BytesContainer bytes) {
    final ResultInternal entity = new ResultInternal(db);
    String fieldName;
    PropertyType type;
    int size = VarIntSerializer.readAsInteger(bytes);
    // fields
    while (size-- > 0) {
      final int len = VarIntSerializer.readAsInteger(bytes);
      // PARSE FIELD NAME
      fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
      bytes.skip(len);
      type = readOType(bytes);

      if (type == null) {
        entity.setProperty(fieldName, null);
      } else {
        final Object value = deserializeValue(db, bytes, type);
        entity.setProperty(fieldName, value);
      }
    }

    int metadataSize = VarIntSerializer.readAsInteger(bytes);
    // metadata
    while (metadataSize-- > 0) {
      final int len = VarIntSerializer.readAsInteger(bytes);
      // PARSE FIELD NAME
      fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
      bytes.skip(len);
      type = readOType(bytes);

      if (type == null) {
        entity.setMetadata(fieldName, null);
      } else {
        final Object value = deserializeValue(db, bytes, type);
        entity.setMetadata(fieldName, value);
      }
    }

    return entity;
  }

  public void serialize(DatabaseSessionInternal db, final Result result,
      final BytesContainer bytes) {
    var propertyNames = result.getPropertyNames();

    VarIntSerializer.write(bytes, propertyNames.size());
    for (String property : propertyNames) {
      writeString(bytes, property);
      Object propertyValue = result.getProperty(property);
      if (propertyValue != null) {
        if (propertyValue instanceof Result) {
          if (((Result) propertyValue).isEntity()) {
            Entity elem = ((Result) propertyValue).getEntity().get();
            writeOType(bytes, bytes.alloc(1), PropertyType.LINK);
            serializeValue(db, bytes, elem.getIdentity(), PropertyType.LINK);
          } else {
            writeOType(bytes, bytes.alloc(1), PropertyType.EMBEDDED);
            serializeValue(db, bytes, propertyValue, PropertyType.EMBEDDED);
          }
        } else {
          final PropertyType type = PropertyType.getTypeByValue(propertyValue);
          if (type == null) {
            throw new SerializationException(
                "Impossible serialize value of type "
                    + propertyValue.getClass()
                    + " with the Result binary serializer");
          }
          writeOType(bytes, bytes.alloc(1), type);
          serializeValue(db, bytes, propertyValue, type);
        }
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }

    Set<String> metadataKeys = result.getMetadataKeys();
    VarIntSerializer.write(bytes, metadataKeys.size());

    for (String field : metadataKeys) {
      writeString(bytes, field);
      final Object value = result.getMetadata(field);
      if (value != null) {
        if (value instanceof Result) {
          writeOType(bytes, bytes.alloc(1), PropertyType.EMBEDDED);
          serializeValue(db, bytes, value, PropertyType.EMBEDDED);
        } else {
          final PropertyType type = PropertyType.getTypeByValue(value);
          if (type == null) {
            throw new SerializationException(
                "Impossible serialize value of type "
                    + value.getClass()
                    + " with the Result binary serializer");
          }
          writeOType(bytes, bytes.alloc(1), type);
          serializeValue(db, bytes, value, type);
        }
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  protected PropertyType readOType(final BytesContainer bytes) {
    byte val = readByte(bytes);
    if (val == -1) {
      return null;
    }
    return PropertyType.getById(val);
  }

  private void writeOType(BytesContainer bytes, int pos, PropertyType type) {
    if (type == null) {
      bytes.bytes[pos] = (byte) -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public Object deserializeValue(DatabaseSessionInternal db, BytesContainer bytes,
      PropertyType type) {
    Object value = null;
    switch (type) {
      case INTEGER:
        value = VarIntSerializer.readAsInteger(bytes);
        break;
      case LONG:
        value = VarIntSerializer.readAsLong(bytes);
        break;
      case SHORT:
        value = VarIntSerializer.readAsShort(bytes);
        break;
      case STRING:
        value = readString(bytes);
        break;
      case DOUBLE:
        value = Double.longBitsToDouble(readLong(bytes));
        break;
      case FLOAT:
        value = Float.intBitsToFloat(readInteger(bytes));
        break;
      case BYTE:
        value = readByte(bytes);
        break;
      case BOOLEAN:
        value = readByte(bytes) == 1;
        break;
      case DATETIME:
        value = new Date(VarIntSerializer.readAsLong(bytes));
        break;
      case DATE:
        long savedTime = VarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
        savedTime =
            convertDayToTimezone(
                TimeZone.getTimeZone("GMT"), DateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
        break;
      case EMBEDDED:
        value = deserialize(db, bytes);
        break;
      case EMBEDDEDSET:
        value = readEmbeddedCollection(db, bytes, new LinkedHashSet<>());
        break;
      case EMBEDDEDLIST:
        value = readEmbeddedCollection(db, bytes, new ArrayList<>());
        break;
      case LINKSET:
        value = readLinkCollection(bytes, new LinkedHashSet<>());
        break;
      case LINKLIST:
        value = readLinkCollection(bytes, new ArrayList<>());
        break;
      case BINARY:
        value = readBinary(bytes);
        break;
      case LINK:
        value = readOptimizedLink(bytes);
        break;
      case LINKMAP:
        value = readLinkMap(db, bytes);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(db, bytes);
        break;
      case DECIMAL:
        value = DecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(DecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        throw new UnsupportedOperationException("LINKBAG should never appear in a projection");
      case TRANSIENT:
        break;
      case CUSTOM:
        try {
          String className = readString(bytes);
          Class<?> clazz = Class.forName(className);
          SerializableStream stream = (SerializableStream) clazz.newInstance();
          stream.fromStream(readBinary(bytes));
          if (stream instanceof SerializableWrapper) {
            value = ((SerializableWrapper) stream).getSerializable();
          } else {
            value = stream;
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      case ANY:
        break;
    }
    return value;
  }

  private byte[] readBinary(BytesContainer bytes) {
    int n = VarIntSerializer.readAsInteger(bytes);
    byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, Identifiable> readLinkMap(DatabaseSessionInternal db,
      final BytesContainer bytes) {
    int size = VarIntSerializer.readAsInteger(bytes);
    Map<Object, Identifiable> result = new HashMap<>();
    while ((size--) > 0) {
      PropertyType keyType = readOType(bytes);
      Object key = deserializeValue(db, bytes, keyType);
      RecordId value = readOptimizedLink(bytes);
      if (value.equals(NULL_RECORD_ID)) {
        result.put(key, null);
      } else {
        result.put(key, value);
      }
    }
    return result;
  }

  private Map readEmbeddedMap(DatabaseSessionInternal db, final BytesContainer bytes) {
    int size = VarIntSerializer.readAsInteger(bytes);
    final Map entity = new LinkedHashMap();
    String fieldName;
    PropertyType type;
    while ((size--) > 0) {
      final int len = VarIntSerializer.readAsInteger(bytes);
      // PARSE FIELD NAME
      fieldName = stringFromBytes(bytes.bytes, bytes.offset, len).intern();
      bytes.skip(len);
      type = readOType(bytes);

      if (type == null) {
        entity.put(fieldName, null);
      } else {
        final Object value = deserializeValue(db, bytes, type);
        entity.put(fieldName, value);
      }
    }
    return entity;
  }

  private static Collection<Identifiable> readLinkCollection(
      BytesContainer bytes, Collection<Identifiable> found) {
    final int items = VarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      RecordId id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.add(null);
      } else {
        found.add(id);
      }
    }
    return found;
  }

  private static RecordId readOptimizedLink(final BytesContainer bytes) {
    return new RecordId(
        VarIntSerializer.readAsInteger(bytes), VarIntSerializer.readAsLong(bytes));
  }

  private Collection<?> readEmbeddedCollection(
      DatabaseSessionInternal db, final BytesContainer bytes, final Collection<Object> found) {
    final int items = VarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      PropertyType itemType = readOType(bytes);
      if (itemType == null) {
        found.add(null);
      } else {
        found.add(deserializeValue(db, bytes, itemType));
      }
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  public void serializeValue(
      DatabaseSessionInternal db, final BytesContainer bytes, Object value,
      final PropertyType type) {

    final int pointer;
    switch (type) {
      case INTEGER:
      case LONG:
      case SHORT:
        VarIntSerializer.write(bytes, ((Number) value).longValue());
        break;
      case STRING:
        writeString(bytes, value.toString());
        break;
      case DOUBLE:
        long dg = Double.doubleToLongBits((Double) value);
        pointer = bytes.alloc(LongSerializer.LONG_SIZE);
        LongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        int fg = Float.floatToIntBits((Float) value);
        pointer = bytes.alloc(IntegerSerializer.INT_SIZE);
        IntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
        break;
      case BYTE:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = (Byte) value;
        break;
      case BOOLEAN:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
        break;
      case DATETIME:
        if (value instanceof Long) {
          VarIntSerializer.write(bytes, (Long) value);
        } else {
          VarIntSerializer.write(bytes, ((Date) value).getTime());
        }
        break;
      case DATE:
        long dateValue;
        if (value instanceof Long) {
          dateValue = (Long) value;
        } else {
          dateValue = ((Date) value).getTime();
        }
        dateValue =
            convertDayToTimezone(
                DateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
        VarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        if (!(value instanceof Result)) {
          throw new UnsupportedOperationException();
        }
        serialize(db, (Result) value, bytes);
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray()) {
          writeEmbeddedCollection(db, bytes, Arrays.asList(MultiValue.array(value)));
        } else {
          writeEmbeddedCollection(db, bytes, (Collection<?>) value);
        }
        break;
      case DECIMAL:
        BigDecimal decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(DecimalSerializer.INSTANCE.getObjectSize(decimalValue));
        DecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
        break;
      case BINARY:
        writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        Collection<Identifiable> ridCollection = (Collection<Identifiable>) value;
        writeLinkCollection(db, bytes, ridCollection);
        break;
      case LINK:
        if (value instanceof Result && ((Result) value).isEntity()) {
          value = ((Result) value).getEntity().get();
        }
        if (!(value instanceof Identifiable)) {
          throw new ValidationException("Value '" + value + "' is not a Identifiable");
        }
        writeOptimizedLink(db, bytes, (Identifiable) value);
        break;
      case LINKMAP:
        writeLinkMap(db, bytes, (Map<Object, Identifiable>) value);
        break;
      case EMBEDDEDMAP:
        writeEmbeddedMap(db, bytes, (Map<Object, Object>) value);
        break;
      case LINKBAG:
        throw new UnsupportedOperationException("LINKBAG should never appear in a projection");
      case CUSTOM:
        if (!(value instanceof SerializableStream)) {
          value = new SerializableWrapper((Serializable) value);
        }
        writeString(bytes, value.getClass().getName());
        writeBinary(bytes, ((SerializableStream) value).toStream());
        break;
      case TRANSIENT:
        break;
      case ANY:
        break;
    }
  }

  private int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = VarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  private void writeLinkMap(DatabaseSessionInternal db, final BytesContainer bytes,
      final Map<Object, Identifiable> map) {
    VarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Identifiable> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      final PropertyType type = PropertyType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(db, bytes, entry.getValue());
      }
    }
  }

  private void writeEmbeddedMap(DatabaseSessionInternal db, BytesContainer bytes,
      Map<Object, Object> map) {
    Set fieldNames = map.keySet();
    VarIntSerializer.write(bytes, map.size());
    for (Object f : fieldNames) {
      if (!(f instanceof String field)) {
        throw new SerializationException(
            "Invalid key type for map: " + f + " (only Strings supported)");
      }
      writeString(bytes, field);
      final Object value = map.get(field);
      if (value != null) {
        if (value instanceof Result) {
          writeOType(bytes, bytes.alloc(1), PropertyType.EMBEDDED);
          serializeValue(db, bytes, value, PropertyType.EMBEDDED);
        } else {
          final PropertyType type = PropertyType.getTypeByValue(value);
          if (type == null) {
            throw new SerializationException(
                "Impossible serialize value of type "
                    + value.getClass()
                    + " with the Result binary serializer");
          }
          writeOType(bytes, bytes.alloc(1), type);
          serializeValue(db, bytes, value, type);
        }
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  private static void writeNullLink(final BytesContainer bytes) {
    VarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    VarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
  }

  private static void writeOptimizedLink(DatabaseSessionInternal db, final BytesContainer bytes,
      Identifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord(db);
      } catch (RecordNotFoundException rnf) {
        // IGNORE THIS
      }
    }
    VarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    VarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
  }

  private static void writeLinkCollection(
      DatabaseSessionInternal db, final BytesContainer bytes,
      final Collection<Identifiable> value) {
    VarIntSerializer.write(bytes, value.size());
    for (Identifiable itemValue : value) {
      if (itemValue == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(db, bytes, itemValue);
      }
    }
  }

  private void writeEmbeddedCollection(DatabaseSessionInternal db, final BytesContainer bytes,
      final Collection<?> value) {
    VarIntSerializer.write(bytes, value.size());

    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), null);
        continue;
      }
      PropertyType type = getTypeFromValueEmbedded(itemValue);
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(db, bytes, itemValue, type);
      } else {
        throw new SerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the EntityImpl binary serializer");
      }
    }
  }

  private static PropertyType getTypeFromValueEmbedded(final Object fieldValue) {
    if (fieldValue instanceof Result && ((Result) fieldValue).isEntity()) {
      return PropertyType.LINK;
    }
    PropertyType type =
        fieldValue instanceof Result ? PropertyType.EMBEDDED
            : PropertyType.getTypeByValue(fieldValue);
    if (type == PropertyType.LINK
        && fieldValue instanceof EntityImpl
        && !((EntityImpl) fieldValue).getIdentity().isValid()) {
      type = PropertyType.EMBEDDED;
    }
    return type;
  }

  protected String readString(final BytesContainer bytes) {
    final int len = VarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected int readInteger(final BytesContainer container) {
    final int value =
        IntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += IntegerSerializer.INT_SIZE;
    return value;
  }

  private byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  private long readLong(final BytesContainer container) {
    final long value =
        LongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += LongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return VarIntSerializer.write(bytes, 0);
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = VarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  private byte[] bytesFromString(final String toWrite) {
    return toWrite.getBytes(StandardCharsets.UTF_8);
  }

  protected String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    return new String(bytes, offset, len, StandardCharsets.UTF_8);
  }

  private long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
    Calendar fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    Calendar toCalendar = Calendar.getInstance(to);
    toCalendar.setTimeInMillis(0);
    toCalendar.set(Calendar.ERA, fromCalendar.get(Calendar.ERA));
    toCalendar.set(Calendar.YEAR, fromCalendar.get(Calendar.YEAR));
    toCalendar.set(Calendar.MONTH, fromCalendar.get(Calendar.MONTH));
    toCalendar.set(Calendar.DAY_OF_MONTH, fromCalendar.get(Calendar.DAY_OF_MONTH));
    toCalendar.set(Calendar.HOUR_OF_DAY, 0);
    toCalendar.set(Calendar.MINUTE, 0);
    toCalendar.set(Calendar.SECOND, 0);
    toCalendar.set(Calendar.MILLISECOND, 0);
    return toCalendar.getTimeInMillis();
  }

  public void toStream(DatabaseSessionInternal db, Result item, ChannelDataOutput channel)
      throws IOException {
    final BytesContainer bytes = new BytesContainer();
    this.serialize(db, item, bytes);
    channel.writeBytes(bytes.fitBytes());
  }

  public ResultInternal fromStream(DatabaseSessionInternal db, ChannelDataInput channel)
      throws IOException {
    BytesContainer bytes = new BytesContainer();
    bytes.bytes = channel.readBytes();
    return this.deserialize(db, bytes);
  }
}
