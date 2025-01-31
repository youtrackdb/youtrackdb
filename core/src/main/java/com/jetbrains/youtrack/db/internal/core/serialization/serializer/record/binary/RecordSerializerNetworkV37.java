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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DecimalSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UUIDSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityEntry;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.EntitySerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.SerializableStream;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.UUID;

public class RecordSerializerNetworkV37 implements RecordSerializerNetwork {

  public static final String NAME = "onet_ser_v37";
  private static final String CHARSET_UTF_8 = "UTF-8";
  protected static final RecordId NULL_RECORD_ID = new RecordId(-2, RID.CLUSTER_POS_INVALID);
  private static final long MILLISEC_PER_DAY = 86400000;
  public static final RecordSerializerNetworkV37 INSTANCE = new RecordSerializerNetworkV37();

  public RecordSerializerNetworkV37() {
  }

  public void deserializePartial(
      DatabaseSessionInternal db, final EntityImpl entity, final BytesContainer bytes,
      final String[] iFields) {
    final var className = readString(bytes);
    if (!className.isEmpty()) {
      EntityInternalUtils.fillClassNameIfNeeded(entity, className);
    }

    String fieldName;
    PropertyType type;
    var size = VarIntSerializer.readAsInteger(bytes);

    var matched = 0;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      Object value;
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(db, bytes, type, entity);
      }
      if (EntityInternalUtils.rawContainsField(entity, fieldName)) {
        continue;
      }
      EntityInternalUtils.rawField(entity, fieldName, value, type);

      for (var field : iFields) {
        if (field.equals(fieldName)) {
          matched++;
        }
      }
      if (matched == iFields.length) {
        break;
      }
    }
  }

  public void deserialize(DatabaseSessionInternal db, final EntityImpl entity,
      final BytesContainer bytes) {
    final var className = readString(bytes);
    if (!className.isEmpty()) {
      EntityInternalUtils.fillClassNameIfNeeded(entity, className);
    }

    String fieldName;
    PropertyType type;
    Object value;
    var size = VarIntSerializer.readAsInteger(bytes);
    while ((size--) > 0) {
      // PARSE FIELD NAME
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(db, bytes, type, entity);
      }
      if (EntityInternalUtils.rawContainsField(entity, fieldName)) {
        continue;
      }
      EntityInternalUtils.rawField(entity, fieldName, value, type);
    }

    RecordInternal.clearSource(entity);
  }

  public void serialize(DatabaseSessionInternal db, final EntityImpl entity,
      final BytesContainer bytes) {
    RecordInternal.checkForBinding(entity);
    serializeClass(entity, bytes);
    final var fields = fetchEntries(entity);
    VarIntSerializer.write(bytes, fields.size());
    for (var entry : fields) {
      var docEntry = entry.getValue();
      writeString(bytes, entry.getKey());
      final var value = docEntry.value;
      if (value != null) {
        final var type = getFieldType(docEntry);
        if (type == null) {
          throw new SerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(db, bytes, value, type, getLinkedType(entity, type, entry.getKey()));
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  protected Collection<Entry<String, EntityEntry>> fetchEntries(EntityImpl entity) {
    return EntityInternalUtils.filteredEntries(entity);
  }

  public String[] getFieldNames(DatabaseSessionInternal db, EntityImpl reference,
      final BytesContainer bytes) {
    // SKIP CLASS NAME
    final var classNameLen = VarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final List<String> result = new ArrayList<String>();

    var size = VarIntSerializer.readAsInteger(bytes);
    String fieldName;
    PropertyType type;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type != null) {
        deserializeValue(db, bytes, type, new EntityImpl(db));
      }
      result.add(fieldName);
    }

    return result.toArray(new String[result.size()]);
  }

  protected SchemaClass serializeClass(final EntityImpl entity, final BytesContainer bytes) {
    final SchemaClass clazz = EntityInternalUtils.getImmutableSchemaClass(entity);
    String name = null;
    if (clazz != null) {
      name = clazz.getName();
    }
    if (name == null) {
      name = entity.getClassName();
    }

    if (name != null) {
      writeString(bytes, name);
    } else {
      writeEmptyString(bytes);
    }
    return clazz;
  }

  protected PropertyType readOType(final BytesContainer bytes) {
    return HelperClasses.readType(bytes);
  }

  private void writeOType(BytesContainer bytes, int pos, PropertyType type) {
    if (type == null) {
      bytes.bytes[pos] = (byte) -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public byte[] serializeValue(DatabaseSessionInternal db, Object value, PropertyType type) {
    var bytes = new BytesContainer();
    serializeValue(db, bytes, value, type, null);
    return bytes.fitBytes();
  }

  public Object deserializeValue(DatabaseSessionInternal db, byte[] val, PropertyType type) {
    var bytes = new BytesContainer(val);
    return deserializeValue(db, bytes, type, null);
  }

  public Object deserializeValue(DatabaseSessionInternal db, BytesContainer bytes,
      PropertyType type,
      RecordElement owner) {
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
        var savedTime = VarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
        savedTime =
            convertDayToTimezone(
                TimeZone.getTimeZone("GMT"), DateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
        break;
      case EMBEDDED:
        value = new EmbeddedEntityImpl(db);
        deserialize(db, (EntityImpl) value, bytes);
        if (((EntityImpl) value).containsField(EntitySerializable.CLASS_NAME)) {
          String className = ((EntityImpl) value).field(EntitySerializable.CLASS_NAME);
          try {
            var clazz = Class.forName(className);
            var newValue = (EntitySerializable) clazz.newInstance();
            newValue.fromDocument((EntityImpl) value);
            value = newValue;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } else {
          EntityInternalUtils.addOwner((EntityImpl) value, owner);
        }

        break;
      case EMBEDDEDSET:
        value = readEmbeddedSet(db, bytes, owner);
        break;
      case EMBEDDEDLIST:
        value = readEmbeddedList(db, bytes, owner);
        break;
      case LINKSET:
        value = readLinkSet(db, bytes, owner);
        break;
      case LINKLIST:
        value = readLinkList(db, bytes, owner);
        break;
      case BINARY:
        value = readBinary(bytes);
        break;
      case LINK:
        value = readOptimizedLink(db, bytes);
        break;
      case LINKMAP:
        value = readLinkMap(db, bytes, owner);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(db, bytes, owner);
        break;
      case DECIMAL:
        value = DecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(DecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        var bag = readRidBag(db, bytes);
        bag.setOwner(owner);
        value = bag;
        break;
      case TRANSIENT:
        break;
      case CUSTOM:
        try {
          var className = readString(bytes);
          var clazz = Class.forName(className);
          var stream = (SerializableStream) clazz.newInstance();
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

  private void writeRidBag(DatabaseSessionInternal db, BytesContainer bytes, RidBag bag) {
    final var bTreeCollectionManager =
        DatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    UUID uuid = null;
    if (bTreeCollectionManager != null) {
      uuid = bTreeCollectionManager.listenForChanges(bag);
    }
    if (uuid == null) {
      uuid = new UUID(-1, -1);
    }
    var uuidPos = bytes.alloc(UUIDSerializer.UUID_SIZE);
    UUIDSerializer.INSTANCE.serialize(uuid, bytes.bytes, uuidPos);

    if (bag.isToSerializeEmbedded()) {
      var pos = bytes.alloc(1);
      bytes.bytes[pos] = 1;
      VarIntSerializer.write(bytes, bag.size());
      for (Identifiable itemValue : bag) {
        if (itemValue == null) {
          writeNullLink(bytes);
        } else {
          writeOptimizedLink(db, bytes, itemValue);
        }
      }
    } else {
      var pos = bytes.alloc(1);
      bytes.bytes[pos] = 2;
      var pointer = bag.getPointer();
      if (pointer == null || pointer == BonsaiCollectionPointer.INVALID) {
        throw new IllegalStateException("RidBag with invalid pointer was found");
      }

      VarIntSerializer.write(bytes, pointer.getFileId());
      VarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
      VarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
      VarIntSerializer.write(bytes, -1);
      var changes = bag.getChanges();
      if (changes != null) {
        VarIntSerializer.write(bytes, changes.size());
        for (var change : changes.entrySet()) {
          writeOptimizedLink(db, bytes, change.getKey());
          var posAll = bytes.alloc(1);
          bytes.bytes[posAll] = change.getValue().getType();
          VarIntSerializer.write(bytes, change.getValue().getValue());
        }
      } else {
        VarIntSerializer.write(bytes, 0);
      }
    }
  }

  protected RidBag readRidBag(DatabaseSessionInternal db, BytesContainer bytes) {
    var uuid = UUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(UUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) {
      uuid = null;
    }
    var b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      var bag = new RidBag(db, uuid);
      // enable tracking due to timeline issue, which must not be NULL (i.e. tracker.isEnabled()).
      bag.enableTracking(null);
      var size = VarIntSerializer.readAsInteger(bytes);
      for (var i = 0; i < size; i++) {
        Identifiable id = readOptimizedLink(db, bytes);
        if (id.equals(NULL_RECORD_ID)) {
          bag.add(null);
        } else {
          bag.add(id.getIdentity());
        }
      }
      bag.disableTracking(null);
      bag.transactionClear();
      return bag;
    } else {
      var fileId = VarIntSerializer.readAsLong(bytes);
      var pageIndex = VarIntSerializer.readAsLong(bytes);
      var pageOffset = VarIntSerializer.readAsInteger(bytes);
      var bagSize = VarIntSerializer.readAsInteger(bytes);

      Map<RID, Change> changes = new HashMap<>();
      var size = VarIntSerializer.readAsInteger(bytes);
      while (size-- > 0) {
        var link = readOptimizedLink(db, bytes);
        var type = bytes.bytes[bytes.offset];
        bytes.skip(1);
        var change = VarIntSerializer.readAsInteger(bytes);
        changes.put(link, ChangeSerializationHelper.createChangeInstance(type, change));
      }

      BonsaiCollectionPointer pointer = null;
      if (fileId != -1) {
        pointer =
            new BonsaiCollectionPointer(fileId, new RidBagBucketPointer(pageIndex, pageOffset));
      }
      return new RidBag(db, pointer, changes, uuid);
    }
  }

  private static byte[] readBinary(BytesContainer bytes) {
    var n = VarIntSerializer.readAsInteger(bytes);
    var newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<String, Identifiable> readLinkMap(
      DatabaseSessionInternal db, final BytesContainer bytes, final RecordElement owner) {
    var size = VarIntSerializer.readAsInteger(bytes);
    var result = new LinkMap(owner);
    while ((size--) > 0) {
      var keyType = readOType(bytes);
      var key = deserializeValue(db, bytes, keyType, result);
      Identifiable value = readOptimizedLink(db, bytes);
      if (value.equals(NULL_RECORD_ID)) {
        result.putInternal(key.toString(), null);
      } else {
        result.putInternal(key.toString(), value);
      }
    }
    return result;
  }

  private Object readEmbeddedMap(DatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    var size = VarIntSerializer.readAsInteger(bytes);
    final TrackedMap result = new TrackedMap<>(owner);
    while ((size--) > 0) {
      var key = readString(bytes);
      var valType = readOType(bytes);
      Object value = null;
      if (valType != null) {
        value = deserializeValue(db, bytes, valType, result);
      }
      result.putInternal(key, value);
    }
    return result;
  }

  private static Collection<Identifiable> readLinkList(
      DatabaseSessionInternal db, BytesContainer bytes, RecordElement owner) {
    var found = new LinkList(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      Identifiable id = readOptimizedLink(db, bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  private static Collection<Identifiable> readLinkSet(DatabaseSessionInternal db,
      BytesContainer bytes,
      RecordElement owner) {
    var found = new LinkSet(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      Identifiable id = readOptimizedLink(db, bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  protected static RID readOptimizedLink(DatabaseSessionInternal db,
      final BytesContainer bytes) {
    var id =
        new RecordId(VarIntSerializer.readAsInteger(bytes), VarIntSerializer.readAsLong(bytes));
    if (id.isTemporary()) {
      try {
        return id.getRecord(db).getIdentity();
      } catch (RecordNotFoundException rnf) {
        return id;
      }
    }

    return id;
  }

  private Collection<?> readEmbeddedList(DatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    var found = new TrackedList<Object>(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      var itemType = readOType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(db, bytes, itemType, found));
      }
    }
    return found;
  }

  private Collection<?> readEmbeddedSet(DatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    var found = new TrackedSet<Object>(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      var itemType = readOType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(db, bytes, itemType, found));
      }
    }
    return found;
  }

  private PropertyType getLinkedType(EntityImpl entity, PropertyType type, String key) {
    if (type != PropertyType.EMBEDDEDLIST && type != PropertyType.EMBEDDEDSET
        && type != PropertyType.EMBEDDEDMAP) {
      return null;
    }
    SchemaClass immutableClass = EntityInternalUtils.getImmutableSchemaClass(entity);
    if (immutableClass != null) {
      var prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public void serializeValue(
      DatabaseSessionInternal db, final BytesContainer bytes, Object value, final PropertyType type,
      final PropertyType linkedType) {
    int pointer;
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
        var dg = Double.doubleToLongBits((Double) value);
        pointer = bytes.alloc(LongSerializer.LONG_SIZE);
        LongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        var fg = Float.floatToIntBits((Float) value);
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
        if (value instanceof EntitySerializable) {
          var cur = ((EntitySerializable) value).toEntity(db);
          cur.field(EntitySerializable.CLASS_NAME, value.getClass().getName());
          serialize(db, cur, bytes);
        } else {
          serialize(db, (EntityImpl) value, bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray()) {
          writeEmbeddedCollection(db, bytes, Arrays.asList(MultiValue.array(value)), linkedType);
        } else {
          writeEmbeddedCollection(db, bytes, (Collection<?>) value, linkedType);
        }
        break;
      case DECIMAL:
        var decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(DecimalSerializer.INSTANCE.getObjectSize(decimalValue));
        DecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
        break;
      case BINARY:
        pointer = writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        var ridCollection = (Collection<Identifiable>) value;
        writeLinkCollection(db, bytes, ridCollection);
        break;
      case LINK:
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
        writeRidBag(db, bytes, (RidBag) value);
        break;
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

  private static int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    return HelperClasses.writeBinary(bytes, valueBytes);
  }

  private void writeLinkMap(DatabaseSessionInternal db, final BytesContainer bytes,
      final Map<Object, Identifiable> map) {
    VarIntSerializer.write(bytes, map.size());
    for (var entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      final var type = PropertyType.STRING;
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
    VarIntSerializer.write(bytes, map.size());
    for (var entry : map.entrySet()) {
      writeString(bytes, entry.getKey().toString());
      final var value = entry.getValue();
      if (value != null) {
        final var type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new SerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(db, bytes, value, type, null);
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  private static void writeNullLink(final BytesContainer bytes) {
    VarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    VarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
  }

  protected void writeOptimizedLink(DatabaseSessionInternal db, final BytesContainer bytes,
      Identifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord(db);
      } catch (RecordNotFoundException rnfe) {
        //
      }
    }
    VarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    VarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
  }

  private void writeLinkCollection(
      DatabaseSessionInternal db, final BytesContainer bytes,
      final Collection<Identifiable> value) {
    VarIntSerializer.write(bytes, value.size());

    for (var itemValue : value) {
      // TODO: handle the null links
      if (itemValue == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(db, bytes, itemValue);
      }
    }
  }

  private void writeEmbeddedCollection(
      DatabaseSessionInternal db, final BytesContainer bytes, final Collection<?> value,
      final PropertyType linkedType) {
    VarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    for (var itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), null);
        continue;
      }
      PropertyType type;
      if (linkedType == null) {
        type = getTypeFromValueEmbedded(itemValue);
      } else {
        type = linkedType;
      }
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(db, bytes, itemValue, type, null);
      } else {
        throw new SerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the EntityImpl binary serializer");
      }
    }
  }

  private PropertyType getFieldType(final EntityEntry entry) {
    var type = entry.type;
    if (type == null) {
      final var prop = entry.property;
      if (prop != null) {
        type = prop.getType();
      }
    }
    if (type == null || PropertyType.ANY == type) {
      type = PropertyType.getTypeByValue(entry.value);
    }
    return type;
  }

  private PropertyType getTypeFromValueEmbedded(final Object fieldValue) {
    var type = PropertyType.getTypeByValue(fieldValue);
    if (type == PropertyType.LINK
        && fieldValue instanceof EntityImpl
        && !((EntityImpl) fieldValue).getIdentity().isValid()) {
      type = PropertyType.EMBEDDED;
    }
    return type;
  }

  protected String readString(final BytesContainer bytes) {
    final var len = VarIntSerializer.readAsInteger(bytes);
    final var res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected int readInteger(final BytesContainer container) {
    final var value =
        IntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += IntegerSerializer.INT_SIZE;
    return value;
  }

  private byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  private long readLong(final BytesContainer container) {
    final var value =
        LongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += LongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return VarIntSerializer.write(bytes, 0);
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final var nameBytes = bytesFromString(toWrite);
    final var pointer = VarIntSerializer.write(bytes, nameBytes.length);
    final var start = bytes.alloc(nameBytes.length);
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
    var fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    var toCalendar = Calendar.getInstance(to);
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

  public RecordAbstract fromStream(DatabaseSessionInternal db, byte[] iSource,
      RecordAbstract record) {
    return fromStream(db, iSource, record, null);
  }

  @Override
  public RecordAbstract fromStream(DatabaseSessionInternal db, byte[] iSource,
      RecordAbstract iRecord, String[] iFields) {
    if (iSource == null || iSource.length == 0) {
      return iRecord;
    }
    if (iRecord == null) {
      iRecord = new EntityImpl(db);
    } else {
      if (iRecord instanceof Blob) {
        RecordInternal.unsetDirty(iRecord);
        RecordInternal.fill(iRecord, iRecord.getIdentity(), iRecord.getVersion(), iSource, true);
        return iRecord;
      }
    }
    RecordInternal.setRecordSerializer(iRecord, this);
    var container = new BytesContainer(iSource);

    try {
      if (iFields != null && iFields.length > 0) {
        deserializePartial(db, (EntityImpl) iRecord, container, iFields);
      } else {
        deserialize(db, (EntityImpl) iRecord, container);
      }
    } catch (RuntimeException e) {
      LogManager.instance()
          .warn(
              this,
              "Error deserializing record with id %s send this data for debugging: %s ",
              iRecord.getIdentity().toString(),
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
    return iRecord;
  }

  @Override
  public byte[] toStream(DatabaseSessionInternal db, RecordAbstract iSource) {
    if (iSource instanceof Blob) {
      return iSource.toStream();
    } else {

      final var container = new BytesContainer();
      var entity = (EntityImpl) iSource;
      // SERIALIZE RECORD
      serialize(db, entity, container);
      return container.fitBytes();
    }
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String[] getFieldNames(DatabaseSessionInternal db, EntityImpl reference,
      byte[] iSource) {
    if (iSource == null || iSource.length == 0) {
      return new String[0];
    }

    final var container = new BytesContainer(iSource);

    try {
      return getFieldNames(db, reference, container);
    } catch (RuntimeException e) {
      LogManager.instance()
          .warn(
              this,
              "Error deserializing record to get field-names, send this data for debugging: %s ",
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
  }
}
