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

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ODecimalSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OLongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OUUIDSerializer;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTValidationException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityEntry;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImplEmbedded;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordFlat;
import com.jetbrains.youtrack.db.internal.core.serialization.ODocumentSerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.OSerializableStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
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
import java.util.NavigableMap;
import java.util.TimeZone;
import java.util.UUID;

public class ORecordSerializerNetworkV37 implements ORecordSerializer {

  public static final String NAME = "onet_ser_v37";
  private static final String CHARSET_UTF_8 = "UTF-8";
  protected static final YTRecordId NULL_RECORD_ID = new YTRecordId(-2, YTRID.CLUSTER_POS_INVALID);
  private static final long MILLISEC_PER_DAY = 86400000;
  public static final ORecordSerializerNetworkV37 INSTANCE = new ORecordSerializerNetworkV37();

  public ORecordSerializerNetworkV37() {
  }

  public void deserializePartial(
      YTDatabaseSessionInternal db, final EntityImpl document, final BytesContainer bytes,
      final String[] iFields) {
    final String className = readString(bytes);
    if (!className.isEmpty()) {
      ODocumentInternal.fillClassNameIfNeeded(document, className);
    }

    String fieldName;
    YTType type;
    int size = OVarIntSerializer.readAsInteger(bytes);

    int matched = 0;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      Object value;
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(db, bytes, type, document);
      }
      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }
      ODocumentInternal.rawField(document, fieldName, value, type);

      for (String field : iFields) {
        if (field.equals(fieldName)) {
          matched++;
        }
      }
      if (matched == iFields.length) {
        break;
      }
    }
  }

  public void deserialize(YTDatabaseSessionInternal db, final EntityImpl document,
      final BytesContainer bytes) {
    final String className = readString(bytes);
    if (!className.isEmpty()) {
      ODocumentInternal.fillClassNameIfNeeded(document, className);
    }

    String fieldName;
    YTType type;
    Object value;
    int size = OVarIntSerializer.readAsInteger(bytes);
    while ((size--) > 0) {
      // PARSE FIELD NAME
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(db, bytes, type, document);
      }
      if (ODocumentInternal.rawContainsField(document, fieldName)) {
        continue;
      }
      ODocumentInternal.rawField(document, fieldName, value, type);
    }

    ORecordInternal.clearSource(document);
  }

  public void serialize(final EntityImpl document, final BytesContainer bytes) {
    ORecordInternal.checkForBinding(document);
    serializeClass(document, bytes);
    final Collection<Entry<String, EntityEntry>> fields = fetchEntries(document);
    OVarIntSerializer.write(bytes, fields.size());
    for (Entry<String, EntityEntry> entry : fields) {
      EntityEntry docEntry = entry.getValue();
      writeString(bytes, entry.getKey());
      final Object value = docEntry.value;
      if (value != null) {
        final YTType type = getFieldType(docEntry);
        if (type == null) {
          throw new YTSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, getLinkedType(document, type, entry.getKey()));
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
  }

  protected Collection<Entry<String, EntityEntry>> fetchEntries(EntityImpl document) {
    return ODocumentInternal.filteredEntries(document);
  }

  public String[] getFieldNames(YTDatabaseSessionInternal db, EntityImpl reference,
      final BytesContainer bytes) {
    // SKIP CLASS NAME
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);

    final List<String> result = new ArrayList<String>();

    int size = OVarIntSerializer.readAsInteger(bytes);
    String fieldName;
    YTType type;
    while ((size--) > 0) {
      fieldName = readString(bytes);
      type = readOType(bytes);
      if (type != null) {
        deserializeValue(db, bytes, type, new EntityImpl());
      }
      result.add(fieldName);
    }

    return result.toArray(new String[result.size()]);
  }

  protected YTClass serializeClass(final EntityImpl document, final BytesContainer bytes) {
    final YTClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    String name = null;
    if (clazz != null) {
      name = clazz.getName();
    }
    if (name == null) {
      name = document.getClassName();
    }

    if (name != null) {
      writeString(bytes, name);
    } else {
      writeEmptyString(bytes);
    }
    return clazz;
  }

  protected YTType readOType(final BytesContainer bytes) {
    return HelperClasses.readType(bytes);
  }

  private void writeOType(BytesContainer bytes, int pos, YTType type) {
    if (type == null) {
      bytes.bytes[pos] = (byte) -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public byte[] serializeValue(Object value, YTType type) {
    BytesContainer bytes = new BytesContainer();
    serializeValue(bytes, value, type, null);
    return bytes.fitBytes();
  }

  public Object deserializeValue(YTDatabaseSessionInternal db, byte[] val, YTType type) {
    BytesContainer bytes = new BytesContainer(val);
    return deserializeValue(db, bytes, type, null);
  }

  public Object deserializeValue(YTDatabaseSessionInternal db, BytesContainer bytes, YTType type,
      RecordElement owner) {
    Object value = null;
    switch (type) {
      case INTEGER:
        value = OVarIntSerializer.readAsInteger(bytes);
        break;
      case LONG:
        value = OVarIntSerializer.readAsLong(bytes);
        break;
      case SHORT:
        value = OVarIntSerializer.readAsShort(bytes);
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
        value = new Date(OVarIntSerializer.readAsLong(bytes));
        break;
      case DATE:
        long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
        savedTime =
            convertDayToTimezone(
                TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
        value = new Date(savedTime);
        break;
      case EMBEDDED:
        value = new EntityImplEmbedded();
        deserialize(db, (EntityImpl) value, bytes);
        if (((EntityImpl) value).containsField(ODocumentSerializable.CLASS_NAME)) {
          String className = ((EntityImpl) value).field(ODocumentSerializable.CLASS_NAME);
          try {
            Class<?> clazz = Class.forName(className);
            ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
            newValue.fromDocument((EntityImpl) value);
            value = newValue;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } else {
          ODocumentInternal.addOwner((EntityImpl) value, owner);
        }

        break;
      case EMBEDDEDSET:
        value = readEmbeddedSet(db, bytes, owner);
        break;
      case EMBEDDEDLIST:
        value = readEmbeddedList(db, bytes, owner);
        break;
      case LINKSET:
        value = readLinkSet(bytes, owner);
        break;
      case LINKLIST:
        value = readLinkList(bytes, owner);
        break;
      case BINARY:
        value = readBinary(bytes);
        break;
      case LINK:
        value = readOptimizedLink(bytes);
        break;
      case LINKMAP:
        value = readLinkMap(db, bytes, owner);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(db, bytes, owner);
        break;
      case DECIMAL:
        value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        RidBag bag = readRidBag(db, bytes);
        bag.setOwner(owner);
        value = bag;
        break;
      case TRANSIENT:
        break;
      case CUSTOM:
        try {
          String className = readString(bytes);
          Class<?> clazz = Class.forName(className);
          OSerializableStream stream = (OSerializableStream) clazz.newInstance();
          stream.fromStream(readBinary(bytes));
          if (stream instanceof OSerializableWrapper) {
            value = ((OSerializableWrapper) stream).getSerializable();
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

  private void writeRidBag(BytesContainer bytes, RidBag bag) {
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    UUID uuid = null;
    if (sbTreeCollectionManager != null) {
      uuid = sbTreeCollectionManager.listenForChanges(bag);
    }
    if (uuid == null) {
      uuid = new UUID(-1, -1);
    }
    int uuidPos = bytes.alloc(OUUIDSerializer.UUID_SIZE);
    OUUIDSerializer.INSTANCE.serialize(uuid, bytes.bytes, uuidPos);
    if (bag.isToSerializeEmbedded()) {
      int pos = bytes.alloc(1);
      bytes.bytes[pos] = 1;
      OVarIntSerializer.write(bytes, bag.size());
      for (YTIdentifiable itemValue : bag) {
        if (itemValue == null) {
          writeNullLink(bytes);
        } else {
          writeOptimizedLink(bytes, itemValue);
        }
      }
    } else {
      int pos = bytes.alloc(1);
      bytes.bytes[pos] = 2;
      OBonsaiCollectionPointer pointer = bag.getPointer();
      if (pointer == null) {
        pointer = OBonsaiCollectionPointer.INVALID;
      }
      OVarIntSerializer.write(bytes, pointer.getFileId());
      OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
      OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
      OVarIntSerializer.write(bytes, -1);
      NavigableMap<YTIdentifiable, Change> changes = bag.getChanges();
      if (changes != null) {
        OVarIntSerializer.write(bytes, changes.size());
        for (Map.Entry<YTIdentifiable, Change> change : changes.entrySet()) {
          writeOptimizedLink(bytes, change.getKey());
          int posAll = bytes.alloc(1);
          bytes.bytes[posAll] = change.getValue().getType();
          OVarIntSerializer.write(bytes, change.getValue().getValue());
        }
      } else {
        OVarIntSerializer.write(bytes, 0);
      }
    }
  }

  protected RidBag readRidBag(YTDatabaseSessionInternal db, BytesContainer bytes) {
    UUID uuid = OUUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OUUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) {
      uuid = null;
    }
    byte b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      RidBag bag = new RidBag(db, uuid);
      // enable tracking due to timeline issue, which must not be NULL (i.e. tracker.isEnabled()).
      bag.enableTracking(null);
      int size = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < size; i++) {
        YTIdentifiable id = readOptimizedLink(bytes);
        if (id.equals(NULL_RECORD_ID)) {
          bag.add(null);
        } else {
          bag.add(id);
        }
      }
      bag.disableTracking(null);
      bag.transactionClear();
      return bag;
    } else {
      long fileId = OVarIntSerializer.readAsLong(bytes);
      long pageIndex = OVarIntSerializer.readAsLong(bytes);
      int pageOffset = OVarIntSerializer.readAsInteger(bytes);
      int bagSize = OVarIntSerializer.readAsInteger(bytes);

      Map<YTIdentifiable, Change> changes = new HashMap<>();
      int size = OVarIntSerializer.readAsInteger(bytes);
      while (size-- > 0) {
        YTIdentifiable link = readOptimizedLink(bytes);
        byte type = bytes.bytes[bytes.offset];
        bytes.skip(1);
        int change = OVarIntSerializer.readAsInteger(bytes);
        changes.put(link, ChangeSerializationHelper.createChangeInstance(type, change));
      }

      OBonsaiCollectionPointer pointer = null;
      if (fileId != -1) {
        pointer =
            new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
      }
      return new RidBag(db, pointer, changes, uuid);
    }
  }

  private byte[] readBinary(BytesContainer bytes) {
    int n = OVarIntSerializer.readAsInteger(bytes);
    byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  private Map<Object, YTIdentifiable> readLinkMap(
      YTDatabaseSessionInternal db, final BytesContainer bytes, final RecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    LinkMap result = new LinkMap(owner);
    while ((size--) > 0) {
      YTType keyType = readOType(bytes);
      Object key = deserializeValue(db, bytes, keyType, result);
      YTIdentifiable value = readOptimizedLink(bytes);
      if (value.equals(NULL_RECORD_ID)) {
        result.putInternal(key, null);
      } else {
        result.putInternal(key, value);
      }
    }
    return result;
  }

  private Object readEmbeddedMap(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final TrackedMap result = new TrackedMap<Object>(owner);
    while ((size--) > 0) {
      String key = readString(bytes);
      YTType valType = readOType(bytes);
      Object value = null;
      if (valType != null) {
        value = deserializeValue(db, bytes, valType, result);
      }
      result.putInternal(key, value);
    }
    return result;
  }

  private static Collection<YTIdentifiable> readLinkList(
      BytesContainer bytes, RecordElement owner) {
    LinkList found = new LinkList(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTIdentifiable id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  private static Collection<YTIdentifiable> readLinkSet(BytesContainer bytes,
      RecordElement owner) {
    LinkSet found = new LinkSet(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTIdentifiable id = readOptimizedLink(bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  protected static YTIdentifiable readOptimizedLink(final BytesContainer bytes) {
    YTRecordId id =
        new YTRecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    if (id.isTemporary()) {
      try {
        return id.getRecord();
      } catch (YTRecordNotFoundException rnf) {
        return id;
      }
    }

    return id;
  }

  private Collection<?> readEmbeddedList(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    TrackedList<Object> found = new TrackedList<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTType itemType = readOType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(db, bytes, itemType, found));
      }
    }
    return found;
  }

  private Collection<?> readEmbeddedSet(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    TrackedSet<Object> found = new TrackedSet<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTType itemType = readOType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(db, bytes, itemType, found));
      }
    }
    return found;
  }

  private YTType getLinkedType(EntityImpl document, YTType type, String key) {
    if (type != YTType.EMBEDDEDLIST && type != YTType.EMBEDDEDSET && type != YTType.EMBEDDEDMAP) {
      return null;
    }
    YTClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      YTProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public void serializeValue(
      final BytesContainer bytes, Object value, final YTType type, final YTType linkedType) {
    int pointer = 0;
    switch (type) {
      case INTEGER:
      case LONG:
      case SHORT:
        OVarIntSerializer.write(bytes, ((Number) value).longValue());
        break;
      case STRING:
        writeString(bytes, value.toString());
        break;
      case DOUBLE:
        long dg = Double.doubleToLongBits((Double) value);
        pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
        OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        int fg = Float.floatToIntBits((Float) value);
        pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
        OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
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
          OVarIntSerializer.write(bytes, (Long) value);
        } else {
          OVarIntSerializer.write(bytes, ((Date) value).getTime());
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
                ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
        OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        if (value instanceof ODocumentSerializable) {
          EntityImpl cur = ((ODocumentSerializable) value).toDocument();
          cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
          serialize(cur, bytes);
        } else {
          serialize((EntityImpl) value, bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray()) {
          writeEmbeddedCollection(bytes, Arrays.asList(OMultiValue.array(value)), linkedType);
        } else {
          writeEmbeddedCollection(bytes, (Collection<?>) value, linkedType);
        }
        break;
      case DECIMAL:
        BigDecimal decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
        ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
        break;
      case BINARY:
        pointer = writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        Collection<YTIdentifiable> ridCollection = (Collection<YTIdentifiable>) value;
        writeLinkCollection(bytes, ridCollection);
        break;
      case LINK:
        if (!(value instanceof YTIdentifiable)) {
          throw new YTValidationException("Value '" + value + "' is not a YTIdentifiable");
        }

        writeOptimizedLink(bytes, (YTIdentifiable) value);
        break;
      case LINKMAP:
        writeLinkMap(bytes, (Map<Object, YTIdentifiable>) value);
        break;
      case EMBEDDEDMAP:
        writeEmbeddedMap(bytes, (Map<Object, Object>) value);
        break;
      case LINKBAG:
        writeRidBag(bytes, (RidBag) value);
        break;
      case CUSTOM:
        if (!(value instanceof OSerializableStream)) {
          value = new OSerializableWrapper((Serializable) value);
        }
        writeString(bytes, value.getClass().getName());
        writeBinary(bytes, ((OSerializableStream) value).toStream());
        break;
      case TRANSIENT:
        break;
      case ANY:
        break;
    }
  }

  private int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  private int writeLinkMap(final BytesContainer bytes, final Map<Object, YTIdentifiable> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, YTIdentifiable> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      final YTType type = YTType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(bytes, entry.getValue());
      }
    }
    return fullPos;
  }

  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      writeString(bytes, entry.getKey().toString());
      final Object value = entry.getValue();
      if (value != null) {
        final YTType type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new YTSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, value, type, null);
      } else {
        writeOType(bytes, bytes.alloc(1), null);
      }
    }
    return fullPos;
  }

  private int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;
  }

  protected void writeOptimizedLink(final BytesContainer bytes, YTIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord();
      } catch (YTRecordNotFoundException rnfe) {
        //
      }
    }
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
  }

  private void writeLinkCollection(
      final BytesContainer bytes, final Collection<YTIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    for (YTIdentifiable itemValue : value) {
      // TODO: handle the null links
      if (itemValue == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(bytes, itemValue);
      }
    }
  }

  private int writeEmbeddedCollection(
      final BytesContainer bytes, final Collection<?> value, final YTType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), null);
        continue;
      }
      YTType type;
      if (linkedType == null) {
        type = getTypeFromValueEmbedded(itemValue);
      } else {
        type = linkedType;
      }
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(bytes, itemValue, type, null);
      } else {
        throw new YTSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the EntityImpl binary serializer");
      }
    }
    return pos;
  }

  private YTType getFieldType(final EntityEntry entry) {
    YTType type = entry.type;
    if (type == null) {
      final YTProperty prop = entry.property;
      if (prop != null) {
        type = prop.getType();
      }
    }
    if (type == null || YTType.ANY == type) {
      type = YTType.getTypeByValue(entry.value);
    }
    return type;
  }

  private YTType getTypeFromValueEmbedded(final Object fieldValue) {
    YTType type = YTType.getTypeByValue(fieldValue);
    if (type == YTType.LINK
        && fieldValue instanceof EntityImpl
        && !((EntityImpl) fieldValue).getIdentity().isValid()) {
      type = YTType.EMBEDDED;
    }
    return type;
  }

  protected String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  protected int readInteger(final BytesContainer container) {
    final int value =
        OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  private byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  private long readLong(final BytesContainer container) {
    final long value =
        OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
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

  public OBinaryField deserializeField(
      final BytesContainer bytes, final YTClass iClass, final String iFieldName) {
    // TODO: check if integrate the binary disc binary comparator here
    throw new UnsupportedOperationException("network serializer doesn't support comparators");
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

  public RecordAbstract fromStream(YTDatabaseSessionInternal db, byte[] iSource,
      RecordAbstract record) {
    return fromStream(db, iSource, record, null);
  }

  @Override
  public RecordAbstract fromStream(YTDatabaseSessionInternal db, byte[] iSource,
      RecordAbstract iRecord, String[] iFields) {
    if (iSource == null || iSource.length == 0) {
      return iRecord;
    }
    if (iRecord == null) {
      iRecord = new EntityImpl();
    } else {
      if (iRecord instanceof Blob) {
        ORecordInternal.unsetDirty(iRecord);
        ORecordInternal.fill(iRecord, iRecord.getIdentity(), iRecord.getVersion(), iSource, true);
        return iRecord;
      } else {
        if (iRecord instanceof RecordFlat) {
          ORecordInternal.unsetDirty(iRecord);
          ORecordInternal.fill(iRecord, iRecord.getIdentity(), iRecord.getVersion(), iSource, true);
          return iRecord;
        }
      }
    }
    ORecordInternal.setRecordSerializer(iRecord, this);
    BytesContainer container = new BytesContainer(iSource);

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
  public byte[] toStream(YTDatabaseSessionInternal session, RecordAbstract iSource) {
    if (iSource instanceof Blob) {
      return iSource.toStream();
    } else {
      if (iSource instanceof RecordFlat) {
        return iSource.toStream();
      } else {
        final BytesContainer container = new BytesContainer();

        EntityImpl doc = (EntityImpl) iSource;
        // SERIALIZE RECORD
        serialize(doc, container);
        return container.fitBytes();
      }
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
  public String[] getFieldNames(YTDatabaseSessionInternal db, EntityImpl reference,
      byte[] iSource) {
    if (iSource == null || iSource.length == 0) {
      return new String[0];
    }

    final BytesContainer container = new BytesContainer(iSource);

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
