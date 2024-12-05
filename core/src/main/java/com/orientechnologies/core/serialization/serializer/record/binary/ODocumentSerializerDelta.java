package com.orientechnologies.core.serialization.serializer.record.binary;

import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.NULL_RECORD_ID;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.getLinkedType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.getTypeFromValueEmbedded;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readBinary;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readOType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeBinary;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeNullLink;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeOType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeString;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OUUIDSerializer;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.LinkList;
import com.orientechnologies.core.db.record.LinkMap;
import com.orientechnologies.core.db.record.LinkSet;
import com.orientechnologies.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.core.db.record.OTrackedMultiValue;
import com.orientechnologies.core.db.record.RecordElement;
import com.orientechnologies.core.db.record.TrackedList;
import com.orientechnologies.core.db.record.TrackedMap;
import com.orientechnologies.core.db.record.TrackedSet;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.exception.YTValidationException;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.EntityEntry;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTEntityImplEmbedded;
import com.orientechnologies.core.serialization.ODocumentSerializable;
import com.orientechnologies.core.serialization.OSerializableStream;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public class ODocumentSerializerDelta {

  protected static final byte CREATED = 1;
  protected static final byte REPLACED = 2;
  protected static final byte CHANGED = 3;
  protected static final byte REMOVED = 4;
  public static final byte DELTA_RECORD_TYPE = 10;

  private static final ODocumentSerializerDelta INSTANCE = new ODocumentSerializerDelta();

  public static ODocumentSerializerDelta instance() {
    return INSTANCE;
  }

  protected ODocumentSerializerDelta() {
  }

  public byte[] serialize(YTEntityImpl document) {
    BytesContainer bytes = new BytesContainer();
    serialize(document, bytes);
    return bytes.fitBytes();
  }

  public byte[] serializeDelta(YTEntityImpl document) {
    BytesContainer bytes = new BytesContainer();
    serializeDelta(bytes, document);
    return bytes.fitBytes();
  }

  protected YTClass serializeClass(final YTEntityImpl document, final BytesContainer bytes) {
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

  private int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  private void serialize(final YTEntityImpl document, final BytesContainer bytes) {
    serializeClass(document, bytes);
    YTClass oClass = ODocumentInternal.getImmutableSchemaClass(document);
    final Set<Map.Entry<String, EntityEntry>> fields = ODocumentInternal.rawEntries(document);
    OVarIntSerializer.write(bytes, document.fields());
    for (Map.Entry<String, EntityEntry> entry : fields) {
      EntityEntry docEntry = entry.getValue();
      if (!docEntry.exists()) {
        continue;
      }
      writeString(bytes, entry.getKey());
      final Object value = entry.getValue().value;
      if (value != null) {
        final YTType type = getFieldType(entry.getValue());
        if (type == null) {
          throw new YTSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeNullableType(bytes, type);
        serializeValue(bytes, value, type, getLinkedType(oClass, type, entry.getKey()));
      } else {
        writeNullableType(bytes, null);
      }
    }
  }

  public void deserialize(YTDatabaseSessionInternal session, byte[] content, YTEntityImpl toFill) {
    BytesContainer bytesContainer = new BytesContainer(content);
    deserialize(session, toFill, bytesContainer);
  }

  private void deserialize(YTDatabaseSessionInternal session, final YTEntityImpl document,
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
      type = readNullableType(bytes);
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(session, bytes, type, document);
      }
      document.setPropertyInternal(fieldName, value, type);
    }
  }

  public void deserializeDelta(YTDatabaseSessionInternal session, byte[] content,
      YTEntityImpl toFill) {
    BytesContainer bytesContainer = new BytesContainer(content);
    deserializeDelta(session, bytesContainer, toFill);
  }

  public void deserializeDelta(YTDatabaseSessionInternal session, BytesContainer bytes,
      YTEntityImpl toFill) {
    final String className = readString(bytes);
    if (!className.isEmpty() && toFill != null) {
      ODocumentInternal.fillClassNameIfNeeded(toFill, className);
    }
    long count = OVarIntSerializer.readAsLong(bytes);
    while (count-- > 0) {
      switch (deserializeByte(bytes)) {
        case CREATED:
          deserializeFullEntry(session, bytes, toFill);
          break;
        case REPLACED:
          deserializeFullEntry(session, bytes, toFill);
          break;
        case CHANGED:
          deserializeDeltaEntry(session, bytes, toFill);
          break;
        case REMOVED:
          String property = readString(bytes);
          if (toFill != null) {
            toFill.removePropertyInternal(property);
          }
          break;
      }
    }
  }

  private void deserializeDeltaEntry(YTDatabaseSessionInternal session, BytesContainer bytes,
      YTEntityImpl toFill) {
    String name = readString(bytes);
    YTType type = readNullableType(bytes);
    Object toUpdate;
    if (toFill != null) {
      toUpdate = toFill.getPropertyInternal(name);
    } else {
      toUpdate = null;
    }
    deserializeDeltaValue(session, bytes, type, toUpdate);
  }

  private void deserializeDeltaValue(YTDatabaseSessionInternal session, BytesContainer bytes,
      YTType type, Object toUpdate) {
    switch (type) {
      case EMBEDDEDLIST:
        deserializeDeltaEmbeddedList(session, bytes, (TrackedList) toUpdate);
        break;
      case EMBEDDEDSET:
        deserializeDeltaEmbeddedSet(session, bytes, (TrackedSet) toUpdate);
        break;
      case EMBEDDEDMAP:
        deserializeDeltaEmbeddedMap(session, bytes, (TrackedMap) toUpdate);
        break;
      case EMBEDDED:
        deserializeDelta(session, bytes, ((YTRecord) toUpdate).getRecord());
        break;
      case LINKLIST:
        deserializeDeltaLinkList(bytes, (LinkList) toUpdate);
        break;
      case LINKSET:
        deserializeDeltaLinkSet(bytes, (LinkSet) toUpdate);
        break;
      case LINKMAP:
        deserializeDeltaLinkMap(bytes, (LinkMap) toUpdate);
        break;
      case LINKBAG:
        deserializeDeltaLinkBag(bytes, (RidBag) toUpdate);
        break;
      default:
        throw new YTSerializationException("delta not supported for type:" + type);
    }
  }

  private void deserializeDeltaLinkMap(BytesContainer bytes, LinkMap toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          String key = readString(bytes);
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.put(key, link);
          }
          break;
        }
        case REPLACED: {
          String key = readString(bytes);
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.put(key, link);
          }
          break;
        }
        case REMOVED: {
          String key = readString(bytes);
          if (toUpdate != null) {
            toUpdate.remove(key);
          }
          break;
        }
      }
    }
  }

  protected void deserializeDeltaLinkBag(BytesContainer bytes, RidBag toUpdate) {
    UUID uuid = OUUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OUUIDSerializer.UUID_SIZE);
    if (toUpdate != null) {
      toUpdate.setTemporaryId(uuid);
    }
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private void deserializeDeltaLinkList(BytesContainer bytes, LinkList toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          long position = OVarIntSerializer.readAsLong(bytes);
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.set((int) position, link);
          }
          break;
        }
        case REMOVED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private void deserializeDeltaLinkSet(BytesContainer bytes, LinkSet toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private void deserializeDeltaEmbeddedMap(YTDatabaseSessionInternal session, BytesContainer bytes,
      TrackedMap toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          String key = readString(bytes);
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.put(key, value);
          }
          break;
        }
        case REPLACED: {
          String key = readString(bytes);
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.put(key, value);
          }
          break;
        }
        case REMOVED:
          String key = readString(bytes);
          if (toUpdate != null) {
            toUpdate.remove(key);
          }
          break;
      }
    }
    long nestedChanges = OVarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      byte other = deserializeByte(bytes);
      assert other == CHANGED;
      String key = readString(bytes);
      Object nested;
      if (toUpdate != null) {
        nested = toUpdate.get(key);
      } else {
        nested = null;
      }
      YTType type = readNullableType(bytes);
      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeDeltaEmbeddedSet(YTDatabaseSessionInternal session, BytesContainer bytes,
      TrackedSet toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.add(value);
          }
          break;
        }
        case REPLACED:
          assert false : "this can't ever happen";
        case REMOVED:
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.remove(value);
          }
          break;
      }
    }
    long nestedChanges = OVarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      byte other = deserializeByte(bytes);
      assert other == CHANGED;
      long position = OVarIntSerializer.readAsLong(bytes);
      YTType type = readNullableType(bytes);
      Object nested;
      if (toUpdate != null) {
        Iterator iter = toUpdate.iterator();
        for (int i = 0; i < position; i++) {
          iter.next();
        }
        nested = iter.next();
      } else {
        nested = null;
      }

      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeDeltaEmbeddedList(YTDatabaseSessionInternal session, BytesContainer bytes,
      TrackedList toUpdate) {
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.add(value);
          }
          break;
        }
        case REPLACED: {
          long pos = OVarIntSerializer.readAsLong(bytes);
          YTType type = readNullableType(bytes);
          Object value;
          if (type != null) {
            value = deserializeValue(session, bytes, type, toUpdate);
          } else {
            value = null;
          }
          if (toUpdate != null) {
            toUpdate.set((int) pos, value);
          }
          break;
        }
        case REMOVED: {
          long pos = OVarIntSerializer.readAsLong(bytes);
          if (toUpdate != null) {
            toUpdate.remove((int) pos);
          }
          break;
        }
      }
    }
    long nestedChanges = OVarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      byte other = deserializeByte(bytes);
      assert other == CHANGED;
      long position = OVarIntSerializer.readAsLong(bytes);
      Object nested;
      if (toUpdate != null) {
        nested = toUpdate.get((int) position);
      } else {
        nested = null;
      }
      YTType type = readNullableType(bytes);
      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeFullEntry(YTDatabaseSessionInternal session, BytesContainer bytes,
      YTEntityImpl toFill) {
    String name = readString(bytes);
    YTType type = readNullableType(bytes);
    Object value;
    if (type != null) {
      value = deserializeValue(session, bytes, type, toFill);
    } else {
      value = null;
    }
    if (toFill != null) {
      toFill.setPropertyInternal(name, value, type);
    }
  }

  public void serializeDelta(BytesContainer bytes, YTEntityImpl document) {
    serializeClass(document, bytes);
    YTClass oClass = ODocumentInternal.getImmutableSchemaClass(document);
    long count =
        ODocumentInternal.rawEntries(document).stream()
            .filter(
                (e) -> {
                  EntityEntry entry = e.getValue();
                  return entry.isTxCreated()
                      || entry.isTxChanged()
                      || entry.isTxTrackedModified()
                      || !entry.isTxExists();
                })
            .count();
    Set<Map.Entry<String, EntityEntry>> entries = ODocumentInternal.rawEntries(document);

    OVarIntSerializer.write(bytes, count);
    for (final Map.Entry<String, EntityEntry> entry : entries) {
      final EntityEntry docEntry = entry.getValue();
      if (!docEntry.isTxExists()) {
        serializeByte(bytes, REMOVED);
        writeString(bytes, entry.getKey());
      } else if (docEntry.isTxCreated()) {
        serializeByte(bytes, CREATED);
        serializeFullEntry(bytes, oClass, entry.getKey(), docEntry);
      } else if (docEntry.isTxChanged()) {
        serializeByte(bytes, REPLACED);
        serializeFullEntry(bytes, oClass, entry.getKey(), docEntry);
      } else if (docEntry.isTxTrackedModified()) {
        serializeByte(bytes, CHANGED);
        // timeline must not be NULL here. Else check that tracker is enabled
        serializeDeltaEntry(bytes, oClass, entry.getKey(), docEntry);
      } else {
        continue;
      }
    }
  }

  private void serializeDeltaEntry(
      BytesContainer bytes, YTClass oClass, String name, EntityEntry entry) {
    final Object value = entry.value;
    assert value != null;
    final YTType type = getFieldType(entry);
    if (type == null) {
      throw new YTSerializationException(
          "Impossible serialize value of type " + value.getClass() + " with the delta serializer");
    }
    writeString(bytes, name);
    writeNullableType(bytes, type);
    serializeDeltaValue(bytes, value, type, getLinkedType(oClass, type, name));
  }

  private void serializeDeltaValue(
      BytesContainer bytes, Object value, YTType type, YTType linkedType) {
    switch (type) {
      case EMBEDDEDLIST:
        serializeDeltaEmbeddedList(bytes, (TrackedList) value);
        break;
      case EMBEDDEDSET:
        serializeDeltaEmbeddedSet(bytes, (TrackedSet) value);
        break;
      case EMBEDDEDMAP:
        serializeDeltaEmbeddedMap(bytes, (TrackedMap) value);
        break;
      case EMBEDDED:
        serializeDelta(bytes, ((YTRecord) value).getRecord());
        break;
      case LINKLIST:
        serializeDeltaLinkList(bytes, (LinkList) value);
        break;
      case LINKSET:
        serializeDeltaLinkSet(bytes, (LinkSet) value);
        break;
      case LINKMAP:
        serializeDeltaLinkMap(bytes, (LinkMap) value);
        break;
      case LINKBAG:
        serializeDeltaLinkBag(bytes, (RidBag) value);
        break;
      default:
        throw new YTSerializationException("delta not supported for type:" + type);
    }
  }

  protected void serializeDeltaLinkBag(BytesContainer bytes, RidBag value) {
    UUID uuid = null;
    YTDatabaseSessionInternal instance = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (instance != null) {
      final OSBTreeCollectionManager sbTreeCollectionManager =
          instance.getSbTreeCollectionManager();
      if (sbTreeCollectionManager != null) {
        uuid = sbTreeCollectionManager.listenForChanges(value);
      }
    }
    if (uuid == null) {
      uuid = new UUID(-1, -1);
    }
    int uuidPos = bytes.alloc(OUUIDSerializer.UUID_SIZE);
    OUUIDSerializer.INSTANCE.serialize(uuid, bytes.bytes, uuidPos);

    final OMultiValueChangeTimeLine<YTIdentifiable, YTIdentifiable> timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for serialization of link types";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkSet(
      BytesContainer bytes, OTrackedMultiValue<YTIdentifiable, YTIdentifiable> value) {
    OMultiValueChangeTimeLine<YTIdentifiable, YTIdentifiable> timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkList(BytesContainer bytes, LinkList value) {
    OMultiValueChangeTimeLine<Integer, YTIdentifiable> timeline = value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<Integer, YTIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          serializeByte(bytes, REPLACED);
          OVarIntSerializer.write(bytes, event.getKey().longValue());
          writeOptimizedLink(bytes, event.getValue());
          break;
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkMap(BytesContainer bytes, LinkMap value) {
    OMultiValueChangeTimeLine<Object, YTIdentifiable> timeline = value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<Object, YTIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeString(bytes, event.getKey().toString());
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          serializeByte(bytes, REPLACED);
          writeString(bytes, event.getKey().toString());
          writeOptimizedLink(bytes, event.getValue());
          break;
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeString(bytes, event.getKey().toString());
          break;
      }
    }
  }

  private void serializeDeltaEmbeddedMap(BytesContainer bytes, TrackedMap value) {
    OMultiValueChangeTimeLine<Object, Object> timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (OMultiValueChangeEvent<Object, Object> event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            writeString(bytes, event.getKey().toString());
            if (event.getValue() != null) {
              YTType type = YTType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case UPDATE: {
            serializeByte(bytes, REPLACED);
            writeString(bytes, event.getKey().toString());
            if (event.getValue() != null) {
              YTType type = YTType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case REMOVE:
            serializeByte(bytes, REMOVED);
            writeString(bytes, event.getKey().toString());
            break;
        }
      }
    } else {
      OVarIntSerializer.write(bytes, 0);
    }
    long count =
        value.values().stream()
            .filter(
                (v) -> {
                  return v instanceof OTrackedMultiValue && ((OTrackedMultiValue) v).isModified()
                      || v instanceof YTEntityImpl
                      && ((YTEntityImpl) v).isEmbedded()
                      && ((YTEntityImpl) v).isDirty();
                })
            .count();
    OVarIntSerializer.write(bytes, count);
    Iterator<Map.Entry<Object, Object>> iterator = value.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Object, Object> singleEntry = iterator.next();
      Object singleValue = singleEntry.getValue();
      if (singleValue instanceof OTrackedMultiValue
          && ((OTrackedMultiValue) singleValue).isModified()) {
        serializeByte(bytes, CHANGED);
        writeString(bytes, singleEntry.getKey().toString());
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      } else if (singleValue instanceof YTEntityImpl
          && ((YTEntityImpl) singleValue).isEmbedded()
          && ((YTEntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        writeString(bytes, singleEntry.getKey().toString());
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      }
    }
  }

  private void serializeDeltaEmbeddedList(BytesContainer bytes, TrackedList value) {
    OMultiValueChangeTimeLine<Integer, Object> timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (OMultiValueChangeEvent<Integer, Object> event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            if (event.getValue() != null) {
              YTType type = YTType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case UPDATE: {
            serializeByte(bytes, REPLACED);
            OVarIntSerializer.write(bytes, event.getKey().longValue());
            if (event.getValue() != null) {
              YTType type = YTType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case REMOVE: {
            serializeByte(bytes, REMOVED);
            OVarIntSerializer.write(bytes, event.getKey().longValue());
            break;
          }
        }
      }
    } else {
      OVarIntSerializer.write(bytes, 0);
    }
    long count =
        value.stream()
            .filter(
                (v) -> {
                  return v instanceof OTrackedMultiValue && ((OTrackedMultiValue) v).isModified()
                      || v instanceof YTEntityImpl
                      && ((YTEntityImpl) v).isEmbedded()
                      && ((YTEntityImpl) v).isDirty();
                })
            .count();
    OVarIntSerializer.write(bytes, count);
    for (int i = 0; i < value.size(); i++) {
      Object singleValue = value.get(i);
      if (singleValue instanceof OTrackedMultiValue
          && ((OTrackedMultiValue) singleValue).isModified()) {
        serializeByte(bytes, CHANGED);
        OVarIntSerializer.write(bytes, i);
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      } else if (singleValue instanceof YTEntityImpl
          && ((YTEntityImpl) singleValue).isEmbedded()
          && ((YTEntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        OVarIntSerializer.write(bytes, i);
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      }
    }
  }

  private void serializeDeltaEmbeddedSet(BytesContainer bytes, TrackedSet value) {
    OMultiValueChangeTimeLine<Object, Object> timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (OMultiValueChangeEvent<Object, Object> event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            if (event.getValue() != null) {
              YTType type = YTType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case UPDATE:
            throw new UnsupportedOperationException(
                "update do not happen in sets, it will be like and add");
          case REMOVE: {
            serializeByte(bytes, REMOVED);
            if (event.getOldValue() != null) {
              YTType type = YTType.getTypeByValue(event.getOldValue());
              writeNullableType(bytes, type);
              serializeValue(bytes, event.getOldValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
        }
      }
    } else {
      OVarIntSerializer.write(bytes, 0);
    }
    long count =
        value.stream()
            .filter(
                (v) -> {
                  return v instanceof OTrackedMultiValue && ((OTrackedMultiValue) v).isModified()
                      || v instanceof YTEntityImpl
                      && ((YTEntityImpl) v).isEmbedded()
                      && ((YTEntityImpl) v).isDirty();
                })
            .count();
    OVarIntSerializer.write(bytes, count);
    int i = 0;
    Iterator<Object> iterator = value.iterator();
    while (iterator.hasNext()) {
      Object singleValue = iterator.next();
      if (singleValue instanceof OTrackedMultiValue
          && ((OTrackedMultiValue) singleValue).isModified()) {
        serializeByte(bytes, CHANGED);
        OVarIntSerializer.write(bytes, i);
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      } else if (singleValue instanceof YTEntityImpl
          && ((YTEntityImpl) singleValue).isEmbedded()
          && ((YTEntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        OVarIntSerializer.write(bytes, i);
        YTType type = YTType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(bytes, singleValue, type, null);
      }
      i++;
    }
  }

  protected YTType getFieldType(final EntityEntry entry) {
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

  private void serializeFullEntry(
      BytesContainer bytes, YTClass oClass, String name, EntityEntry entry) {
    final Object value = entry.value;
    if (value != null) {
      final YTType type = getFieldType(entry);
      if (type == null) {
        throw new YTSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the delta serializer");
      }
      writeString(bytes, name);
      writeNullableType(bytes, type);
      serializeValue(bytes, value, type, getLinkedType(oClass, type, name));
    } else {
      writeString(bytes, name);
      writeNullableType(bytes, null);
    }
  }

  protected byte deserializeByte(BytesContainer bytes) {
    int pos = bytes.offset;
    bytes.skip(1);
    return bytes.bytes[pos];
  }

  protected void serializeByte(BytesContainer bytes, byte value) {
    int pointer = bytes.alloc(1);
    bytes.bytes[pointer] = value;
  }

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
          YTEntityImpl cur = ((ODocumentSerializable) value).toDocument();
          cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
          serialize(cur, bytes);
        } else {
          serialize(((YTRecord) value).getRecord(), bytes);
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

  private int writeLinkCollection(
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

    return pos;
  }

  private int writeLinkMap(final BytesContainer bytes, final Map<Object, YTIdentifiable> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Map.Entry<Object, YTIdentifiable> entry : map.entrySet()) {
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

  private int writeEmbeddedCollection(
      final BytesContainer bytes, final Collection<?> value, final YTType linkedType) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeNullableType(bytes, null);
        continue;
      }
      YTType type;
      if (linkedType == null) {
        type = getTypeFromValueEmbedded(itemValue);
      } else {
        type = linkedType;
      }
      if (type != null) {
        writeNullableType(bytes, type);
        serializeValue(bytes, itemValue, type, null);
      } else {
        throw new YTSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the YTEntityImpl binary serializer");
      }
    }
    return pos;
  }

  private int writeEmbeddedMap(BytesContainer bytes, Map<Object, Object> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
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
        writeNullableType(bytes, type);
        serializeValue(bytes, value, type, null);
      } else {
        writeNullableType(bytes, null);
      }
    }
    return fullPos;
  }

  public Object deserializeValue(YTDatabaseSessionInternal session, BytesContainer bytes,
      YTType type,
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
        value = new YTEntityImplEmbedded();
        deserialize(session, (YTEntityImpl) value, bytes);
        if (((YTEntityImpl) value).containsField(ODocumentSerializable.CLASS_NAME)) {
          String className = ((YTEntityImpl) value).field(ODocumentSerializable.CLASS_NAME);
          try {
            Class<?> clazz = Class.forName(className);
            ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
            newValue.fromDocument((YTEntityImpl) value);
            value = newValue;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } else {
          ODocumentInternal.addOwner((YTEntityImpl) value, owner);
        }

        break;
      case EMBEDDEDSET:
        value = readEmbeddedSet(session, bytes, owner);
        break;
      case EMBEDDEDLIST:
        value = readEmbeddedList(session, bytes, owner);
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
        value = readLinkMap(session, bytes, owner);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(session, bytes, owner);
        break;
      case DECIMAL:
        value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        RidBag bag = readRidBag(session, bytes);
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

  private Collection<?> readEmbeddedList(YTDatabaseSessionInternal session,
      final BytesContainer bytes, final RecordElement owner) {
    TrackedList<Object> found = new TrackedList<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTType itemType = readNullableType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(session, bytes, itemType, found));
      }
    }
    return found;
  }

  private Collection<?> readEmbeddedSet(YTDatabaseSessionInternal session,
      final BytesContainer bytes, final RecordElement owner) {
    TrackedSet<Object> found = new TrackedSet<>(owner);
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTType itemType = readNullableType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(session, bytes, itemType, found));
      }
    }
    return found;
  }

  private Collection<YTIdentifiable> readLinkList(BytesContainer bytes, RecordElement owner) {
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

  private Collection<YTIdentifiable> readLinkSet(BytesContainer bytes, RecordElement owner) {
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

  private Map<Object, YTIdentifiable> readLinkMap(
      YTDatabaseSessionInternal session, final BytesContainer bytes, final RecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    LinkMap result = new LinkMap(owner);
    while ((size--) > 0) {
      YTType keyType = readOType(bytes, false);
      Object key = deserializeValue(session, bytes, keyType, result);
      YTIdentifiable value = readOptimizedLink(bytes);
      if (value.equals(NULL_RECORD_ID)) {
        result.putInternal(key, null);
      } else {
        result.putInternal(key, value);
      }
    }

    return result;
  }

  private Object readEmbeddedMap(YTDatabaseSessionInternal session, final BytesContainer bytes,
      final RecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final TrackedMap result = new TrackedMap<Object>(owner);
    while ((size--) > 0) {
      String key = readString(bytes);
      YTType valType = readNullableType(bytes);
      Object value = null;
      if (valType != null) {
        value = deserializeValue(session, bytes, valType, result);
      }
      result.putInternal(key, value);
    }
    return result;
  }

  private RidBag readRidBag(YTDatabaseSessionInternal session, BytesContainer bytes) {
    UUID uuid = OUUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OUUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) {
      uuid = null;
    }
    byte b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      RidBag bag = new RidBag(session, uuid);
      int size = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < size; i++) {
        YTIdentifiable id = readOptimizedLink(bytes);
        if (id.equals(NULL_RECORD_ID)) {
          bag.add(null);
        } else {
          bag.add(id);
        }
      }
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
      return new RidBag(session, pointer, changes, uuid);
    }
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
      Iterator<YTIdentifiable> iterator = bag.iterator();
      while (iterator.hasNext()) {
        YTIdentifiable itemValue = iterator.next();
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
      OVarIntSerializer.write(bytes, bag.size());
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

  public static void writeNullableType(BytesContainer bytes, YTType type) {
    int pos = bytes.alloc(1);
    if (type == null) {
      bytes.bytes[pos] = -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public static YTType readNullableType(BytesContainer bytes) {
    byte typeId = bytes.bytes[bytes.offset++];
    if (typeId == -1) {
      return null;
    }
    return YTType.getById(typeId);
  }

  public static YTRecordId readOptimizedLink(final BytesContainer bytes) {
    int clusterId = OVarIntSerializer.readAsInteger(bytes);
    long clusterPos = OVarIntSerializer.readAsLong(bytes);

    if (clusterId == -2 && clusterPos == -2) {
      return null;
    } else {
      var rid = new YTRecordId(clusterId, clusterPos);

      if (rid.isTemporary()) {
        try {
          // rid will be changed during commit we need to keep track original rid
          var record = rid.getRecord();

          rid = (YTRecordId) record.getIdentity();
          if (rid == null) {
            rid = new YTRecordId(clusterId, clusterPos);
          }

        } catch (YTRecordNotFoundException rnf) {
          return rid;
        }
      }

      return rid;
    }
  }

  public static void writeOptimizedLink(final BytesContainer bytes, YTIdentifiable link) {
    if (link == null) {
      OVarIntSerializer.write(bytes, -2);
      OVarIntSerializer.write(bytes, -2);
    } else {
      if (!link.getIdentity().isPersistent()) {
        try {
          link = link.getRecord();
        } catch (YTRecordNotFoundException ignored) {
          // IGNORE IT WILL FAIL THE ASSERT IN CASE
        }
      }

      OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
      OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    }
  }
}
