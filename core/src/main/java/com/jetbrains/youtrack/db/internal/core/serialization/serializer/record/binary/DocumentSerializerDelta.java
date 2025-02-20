package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.NULL_RECORD_ID;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.getLinkedType;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.getTypeFromValueEmbedded;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readBinary;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readOType;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.writeBinary;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.writeNullLink;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.writeOType;
import static com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.HelperClasses.writeString;

import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.serialization.types.DecimalSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UUIDSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityEntry;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.EntitySerializable;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import javax.annotation.Nonnull;

public class DocumentSerializerDelta {

  protected static final byte CREATED = 1;
  protected static final byte REPLACED = 2;
  protected static final byte CHANGED = 3;
  protected static final byte REMOVED = 4;
  public static final byte DELTA_RECORD_TYPE = 10;

  private static final DocumentSerializerDelta INSTANCE = new DocumentSerializerDelta();

  public static DocumentSerializerDelta instance() {
    return INSTANCE;
  }

  protected DocumentSerializerDelta() {
  }

  public byte[] serialize(DatabaseSessionInternal session, EntityImpl entity) {
    var bytes = new BytesContainer();
    serialize(session, entity, bytes);
    return bytes.fitBytes();
  }

  public byte[] serializeDelta(DatabaseSessionInternal session, EntityImpl entity) {
    var bytes = new BytesContainer();
    serializeDelta(session, bytes, entity);
    return bytes.fitBytes();
  }

  protected void serializeClass(DatabaseSessionInternal session, final EntityImpl entity,
      final BytesContainer bytes) {
    SchemaImmutableClass result = null;
    if (entity != null) {
      result = entity.getImmutableSchemaClass(session);
    }
    final SchemaClass clazz = result;
    String name = null;
    if (clazz != null) {
      name = clazz.getName(session);
    }
    if (name == null) {
      name = entity.getSchemaClassName();
    }

    if (name != null) {
      writeString(bytes, name);
    } else {
      writeEmptyString(bytes);
    }
  }

  private int writeEmptyString(final BytesContainer bytes) {
    return VarIntSerializer.write(bytes, 0);
  }

  private void serialize(DatabaseSessionInternal session, final EntityImpl entity,
      final BytesContainer bytes) {
    serializeClass(session, entity, bytes);
    SchemaImmutableClass result = null;
    if (entity != null) {
      result = entity.getImmutableSchemaClass(session);
    }
    SchemaClass oClass = result;
    final var fields = EntityInternalUtils.rawEntries(entity);
    VarIntSerializer.write(bytes, entity.fields());
    for (var entry : fields) {
      var entityEntry = entry.getValue();
      if (!entityEntry.exists()) {
        continue;
      }
      writeString(bytes, entry.getKey());
      final var value = entry.getValue().value;
      if (value != null) {
        final var type = getFieldType(session, entry.getValue());
        if (type == null) {
          throw new SerializationException(session.getDatabaseName(),
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeNullableType(bytes, type);
        serializeValue(session, bytes, value, type,
            getLinkedType(session, oClass, type, entry.getKey()));
      } else {
        writeNullableType(bytes, null);
      }
    }
  }

  public void deserialize(DatabaseSessionInternal session, byte[] content, EntityImpl toFill) {
    var bytesContainer = new BytesContainer(content);
    deserialize(session, toFill, bytesContainer);
  }

  private void deserialize(DatabaseSessionInternal session, final EntityImpl entity,
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
      type = readNullableType(bytes);
      if (type == null) {
        value = null;
      } else {
        value = deserializeValue(session, bytes, type, entity);
      }
      entity.setPropertyInternal(fieldName, value, type);
    }
  }

  public void deserializeDelta(DatabaseSessionInternal session, byte[] content,
      EntityImpl toFill) {
    var bytesContainer = new BytesContainer(content);
    deserializeDelta(session, bytesContainer, toFill);
  }

  public void deserializeDelta(DatabaseSessionInternal session, BytesContainer bytes,
      EntityImpl toFill) {
    final var className = readString(bytes);
    if (!className.isEmpty() && toFill != null) {
      EntityInternalUtils.fillClassNameIfNeeded(toFill, className);
    }
    var count = VarIntSerializer.readAsLong(bytes);
    while (count-- > 0) {
      switch (deserializeByte(bytes)) {
        case CREATED, REPLACED:
          deserializeFullEntry(session, bytes, toFill);
          break;
        case CHANGED:
          deserializeDeltaEntry(session, bytes, toFill);
          break;
        case REMOVED:
          var property = readString(bytes);
          if (toFill != null) {
            toFill.removePropertyInternal(property);
          }
          break;
      }
    }
  }

  private void deserializeDeltaEntry(DatabaseSessionInternal session, BytesContainer bytes,
      EntityImpl toFill) {
    var name = readString(bytes);
    var type = readNullableType(bytes);
    Object toUpdate;
    if (toFill != null) {
      toUpdate = toFill.getPropertyInternal(name);
    } else {
      toUpdate = null;
    }
    deserializeDeltaValue(session, bytes, type, toUpdate);
  }

  private void deserializeDeltaValue(DatabaseSessionInternal session, BytesContainer bytes,
      PropertyType type, Object toUpdate) {
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
        deserializeDelta(session, bytes, ((DBRecord) toUpdate).getRecord(session));
        break;
      case LINKLIST:
        deserializeDeltaLinkList(session, bytes, (LinkList) toUpdate);
        break;
      case LINKSET:
        deserializeDeltaLinkSet(session, bytes, (LinkSet) toUpdate);
        break;
      case LINKMAP:
        deserializeDeltaLinkMap(session, bytes, (LinkMap) toUpdate);
        break;
      case LINKBAG:
        deserializeDeltaLinkBag(session, bytes, (RidBag) toUpdate);
        break;
      default:
        throw new SerializationException(session.getDatabaseName(),
            "delta not supported for type:" + type);
    }
  }

  private static void deserializeDeltaLinkMap(DatabaseSessionInternal session, BytesContainer bytes,
      LinkMap toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var key = readString(bytes);
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.put(key, link);
          }
          break;
        }
        case REPLACED: {
          var key = readString(bytes);
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.put(key, link);
          }
          break;
        }
        case REMOVED: {
          var key = readString(bytes);
          if (toUpdate != null) {
            toUpdate.remove(key);
          }
          break;
        }
      }
    }
  }

  protected void deserializeDeltaLinkBag(DatabaseSessionInternal session, BytesContainer bytes,
      RidBag toUpdate) {
    var uuid = UUIDSerializer.INSTANCE.deserialize(session.getSerializerFactory(), bytes.bytes,
        bytes.offset);
    bytes.skip(UUIDSerializer.UUID_SIZE);
    if (toUpdate != null) {
      toUpdate.setTemporaryId(uuid);
    }
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private static void deserializeDeltaLinkList(DatabaseSessionInternal session,
      BytesContainer bytes,
      LinkList toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          var position = VarIntSerializer.readAsLong(bytes);
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.set((int) position, link);
          }
          break;
        }
        case REMOVED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private static void deserializeDeltaLinkSet(DatabaseSessionInternal session, BytesContainer bytes,
      LinkSet toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          var link = readOptimizedLink(session, bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
  }

  private void deserializeDeltaEmbeddedMap(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedMap toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var key = readString(bytes);
          var type = readNullableType(bytes);
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
          var key = readString(bytes);
          var type = readNullableType(bytes);
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
          var key = readString(bytes);
          if (toUpdate != null) {
            toUpdate.remove(key);
          }
          break;
      }
    }
    var nestedChanges = VarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      var other = deserializeByte(bytes);
      assert other == CHANGED;
      var key = readString(bytes);
      Object nested;
      if (toUpdate != null) {
        nested = toUpdate.get(key);
      } else {
        nested = null;
      }
      var type = readNullableType(bytes);
      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeDeltaEmbeddedSet(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedSet toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var type = readNullableType(bytes);
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
          var type = readNullableType(bytes);
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
    var nestedChanges = VarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      var other = deserializeByte(bytes);
      assert other == CHANGED;
      var position = VarIntSerializer.readAsLong(bytes);
      var type = readNullableType(bytes);
      Object nested;
      if (toUpdate != null) {
        var iter = toUpdate.iterator();
        for (var i = 0; i < position; i++) {
          iter.next();
        }
        nested = iter.next();
      } else {
        nested = null;
      }

      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeDeltaEmbeddedList(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedList toUpdate) {
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var type = readNullableType(bytes);
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
          var pos = VarIntSerializer.readAsLong(bytes);
          var type = readNullableType(bytes);
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
          var pos = VarIntSerializer.readAsLong(bytes);
          if (toUpdate != null) {
            toUpdate.remove((int) pos);
          }
          break;
        }
      }
    }
    var nestedChanges = VarIntSerializer.readAsLong(bytes);
    while (nestedChanges-- > 0) {
      var other = deserializeByte(bytes);
      assert other == CHANGED;
      var position = VarIntSerializer.readAsLong(bytes);
      Object nested;
      if (toUpdate != null) {
        nested = toUpdate.get((int) position);
      } else {
        nested = null;
      }
      var type = readNullableType(bytes);
      deserializeDeltaValue(session, bytes, type, nested);
    }
  }

  private void deserializeFullEntry(DatabaseSessionInternal session, BytesContainer bytes,
      EntityImpl toFill) {
    var name = readString(bytes);
    var type = readNullableType(bytes);
    Object value;
    if (type != null) {
      value = deserializeValue(session, bytes, type, toFill);
    } else {
      value = null;
    }
    if (toFill != null) {
      toFill.compareAndSetPropertyInternal(name, value, type);
    }
  }

  public void serializeDelta(DatabaseSessionInternal session, BytesContainer bytes,
      EntityImpl entity) {
    serializeClass(session, entity, bytes);
    SchemaImmutableClass result = null;
    if (entity != null) {
      result = entity.getImmutableSchemaClass(session);
    }
    SchemaClass oClass = result;
    var count =
        EntityInternalUtils.rawEntries(entity).stream()
            .filter(
                (e) -> {
                  var entry = e.getValue();
                  return entry.isTxCreated()
                      || entry.isTxChanged()
                      || entry.isTxTrackedModified()
                      || !entry.isTxExists();
                })
            .count();
    var entries = EntityInternalUtils.rawEntries(entity);

    VarIntSerializer.write(bytes, count);
    for (final var entry : entries) {
      final var docEntry = entry.getValue();
      if (!docEntry.isTxExists()) {
        serializeByte(bytes, REMOVED);
        writeString(bytes, entry.getKey());
      } else if (docEntry.isTxCreated()) {
        serializeByte(bytes, CREATED);
        serializeFullEntry(session, bytes, oClass, entry.getKey(), docEntry);
      } else if (docEntry.isTxChanged()) {
        serializeByte(bytes, REPLACED);
        serializeFullEntry(session, bytes, oClass, entry.getKey(), docEntry);
      } else if (docEntry.isTxTrackedModified()) {
        serializeByte(bytes, CHANGED);
        // timeline must not be NULL here. Else check that tracker is enabled
        serializeDeltaEntry(session, bytes, oClass, entry.getKey(), docEntry);
      } else {
        continue;
      }
    }
  }

  private void serializeDeltaEntry(
      DatabaseSessionInternal session, BytesContainer bytes, SchemaClass oClass, String name,
      EntityEntry entry) {
    final var value = entry.value;
    assert value != null;
    final var type = getFieldType(session, entry);
    if (type == null) {
      throw new SerializationException(session.getDatabaseName(),
          "Impossible serialize value of type " + value.getClass() + " with the delta serializer");
    }
    writeString(bytes, name);
    writeNullableType(bytes, type);
    serializeDeltaValue(session, bytes, value, type, getLinkedType(session, oClass, type, name));
  }

  private void serializeDeltaValue(
      DatabaseSessionInternal session, BytesContainer bytes, Object value, PropertyType type,
      PropertyType linkedType) {
    switch (type) {
      case EMBEDDEDLIST:
        serializeDeltaEmbeddedList(session, bytes, (TrackedList) value);
        break;
      case EMBEDDEDSET:
        serializeDeltaEmbeddedSet(session, bytes, (TrackedSet) value);
        break;
      case EMBEDDEDMAP:
        serializeDeltaEmbeddedMap(session, bytes, (TrackedMap) value);
        break;
      case EMBEDDED:
        serializeDelta(session, bytes, ((DBRecord) value).getRecord(session));
        break;
      case LINKLIST:
        serializeDeltaLinkList(session, bytes, (LinkList) value);
        break;
      case LINKSET:
        serializeDeltaLinkSet(session, bytes, (LinkSet) value);
        break;
      case LINKMAP:
        serializeDeltaLinkMap(session, bytes, (LinkMap) value);
        break;
      case LINKBAG:
        serializeDeltaLinkBag(session, bytes, (RidBag) value);
        break;
      default:
        throw new SerializationException(session.getDatabaseName(),
            "delta not supported for type:" + type);
    }
  }

  protected void serializeDeltaLinkBag(DatabaseSessionInternal session, BytesContainer bytes,
      RidBag value) {
    UUID uuid = null;
    final var bTreeCollectionManager = session.getSbTreeCollectionManager();
    if (bTreeCollectionManager != null) {
      uuid = bTreeCollectionManager.listenForChanges(value, session);
    }

    if (uuid == null) {
      uuid = new UUID(-1, -1);
    }
    var uuidPos = bytes.alloc(UUIDSerializer.UUID_SIZE);
    UUIDSerializer.INSTANCE.serialize(uuid, session.getSerializerFactory(), bytes.bytes, uuidPos);

    final var timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for serialization of link types";
    VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (var event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(session, bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkSet(
      DatabaseSessionInternal session, BytesContainer bytes,
      TrackedMultiValue<Identifiable, Identifiable> value) {
    var timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (var event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(session, bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkList(DatabaseSessionInternal session, BytesContainer bytes,
      LinkList value) {
    var timeline = value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (var event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case UPDATE:
          serializeByte(bytes, REPLACED);
          VarIntSerializer.write(bytes, event.getKey().longValue());
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(session, bytes, event.getOldValue());
          break;
      }
    }
  }

  private void serializeDeltaLinkMap(DatabaseSessionInternal session, BytesContainer bytes,
      LinkMap value) {
    var timeline = value.getTransactionTimeLine();
    assert timeline != null : "Collection timeline required for link* types serialization";
    VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (var event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeString(bytes, event.getKey());
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case UPDATE:
          serializeByte(bytes, REPLACED);
          writeString(bytes, event.getKey());
          writeOptimizedLink(session, bytes, event.getValue());
          break;
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeString(bytes, event.getKey());
          break;
      }
    }
  }

  private void serializeDeltaEmbeddedMap(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedMap<?> value) {
    var timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (var event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            writeString(bytes, event.getKey());
            if (event.getValue() != null) {
              var type = PropertyType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case UPDATE: {
            serializeByte(bytes, REPLACED);
            writeString(bytes, event.getKey());
            if (event.getValue() != null) {
              var type = PropertyType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case REMOVE:
            serializeByte(bytes, REMOVED);
            writeString(bytes, event.getKey());
            break;
        }
      }
    } else {
      VarIntSerializer.write(bytes, 0);
    }
    var count =
        value.values().stream()
            .filter(
                (v) -> {
                  return v instanceof TrackedMultiValue<?, ?> trackedMultiValue &&
                      trackedMultiValue.isTransactionModified()
                      || v instanceof EntityImpl
                      && ((EntityImpl) v).isEmbedded()
                      && ((EntityImpl) v).isDirty();
                })
            .count();
    VarIntSerializer.write(bytes, count);
    for (var singleEntry : value.entrySet()) {
      var singleValue = singleEntry.getValue();
      if (singleValue instanceof TrackedMultiValue<?, ?> trackedMultiValue
          && trackedMultiValue.isTransactionModified()) {
        serializeByte(bytes, CHANGED);
        writeString(bytes, singleEntry.getKey());
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      } else if (singleValue instanceof EntityImpl
          && ((EntityImpl) singleValue).isEmbedded()
          && ((EntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        writeString(bytes, singleEntry.getKey());
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      }
    }
  }

  private void serializeDeltaEmbeddedList(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedList<?> value) {
    var timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (var event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            if (event.getValue() != null) {
              var type = PropertyType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case UPDATE: {
            serializeByte(bytes, REPLACED);
            VarIntSerializer.write(bytes, event.getKey().longValue());
            if (event.getValue() != null) {
              var type = PropertyType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
          case REMOVE: {
            serializeByte(bytes, REMOVED);
            VarIntSerializer.write(bytes, event.getKey().longValue());
            break;
          }
        }
      }
    } else {
      VarIntSerializer.write(bytes, 0);
    }
    var count =
        value.stream()
            .filter(
                (v) -> {
                  return v instanceof TrackedMultiValue<?, ?> trackedMultiValue
                      && trackedMultiValue.isTransactionModified()
                      || v instanceof EntityImpl
                      && ((EntityImpl) v).isEmbedded()
                      && ((EntityImpl) v).isDirty();
                })
            .count();
    VarIntSerializer.write(bytes, count);
    for (var i = 0; i < value.size(); i++) {
      var singleValue = value.get(i);
      if (singleValue instanceof TrackedMultiValue<?, ?> trackedMultiValue
          && trackedMultiValue.isTransactionModified()) {
        serializeByte(bytes, CHANGED);
        VarIntSerializer.write(bytes, i);
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      } else if (singleValue instanceof EntityImpl
          && ((EntityImpl) singleValue).isEmbedded()
          && ((EntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        VarIntSerializer.write(bytes, i);
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      }
    }
  }

  private void serializeDeltaEmbeddedSet(DatabaseSessionInternal session, BytesContainer bytes,
      TrackedSet<?> value) {
    var timeline = value.getTransactionTimeLine();
    if (timeline != null) {
      VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
      for (var event : timeline.getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD: {
            serializeByte(bytes, CREATED);
            if (event.getValue() != null) {
              var type = PropertyType.getTypeByValue(event.getValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getValue(), type, null);
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
              var type = PropertyType.getTypeByValue(event.getOldValue());
              writeNullableType(bytes, type);
              serializeValue(session, bytes, event.getOldValue(), type, null);
            } else {
              writeNullableType(bytes, null);
            }
            break;
          }
        }
      }
    } else {
      VarIntSerializer.write(bytes, 0);
    }
    var count =
        value.stream()
            .filter(
                (v) -> v instanceof TrackedMultiValue<?, ?> trackedMultiValue
                    && trackedMultiValue.isTransactionModified()
                    || v instanceof EntityImpl
                    && ((EntityImpl) v).isEmbedded()
                    && ((EntityImpl) v).isDirty())
            .count();
    VarIntSerializer.write(bytes, count);
    var i = 0;
    for (var singleValue : value) {
      if (singleValue instanceof TrackedMultiValue<?, ?> trackedMultiValue
          && trackedMultiValue.isTransactionModified()) {
        serializeByte(bytes, CHANGED);
        VarIntSerializer.write(bytes, i);
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      } else if (singleValue instanceof EntityImpl
          && ((EntityImpl) singleValue).isEmbedded()
          && ((EntityImpl) singleValue).isDirty()) {
        serializeByte(bytes, CHANGED);
        VarIntSerializer.write(bytes, i);
        var type = PropertyType.getTypeByValue(singleValue);
        writeNullableType(bytes, type);
        serializeDeltaValue(session, bytes, singleValue, type, null);
      }
      i++;
    }
  }

  protected static PropertyType getFieldType(DatabaseSessionInternal session,
      final EntityEntry entry) {
    var type = entry.type;
    if (type == null) {
      final var prop = entry.property;
      if (prop != null) {
        type = prop.getType(session);
      }
    }
    if (type == null || PropertyType.ANY == type) {
      type = PropertyType.getTypeByValue(entry.value);
    }
    return type;
  }

  private void serializeFullEntry(
      @Nonnull DatabaseSessionInternal session, BytesContainer bytes, SchemaClass oClass,
      String name,
      EntityEntry entry) {
    final var value = entry.value;
    if (value != null) {
      final var type = getFieldType(session, entry);
      if (type == null) {
        throw new SerializationException(session.getDatabaseName(),
            "Impossible serialize value of type "
                + value.getClass()
                + " with the delta serializer");
      }
      writeString(bytes, name);
      writeNullableType(bytes, type);
      serializeValue(session, bytes, value, type, getLinkedType(session, oClass, type, name));
    } else {
      writeString(bytes, name);
      writeNullableType(bytes, null);
    }
  }

  protected static byte deserializeByte(BytesContainer bytes) {
    var pos = bytes.offset;
    bytes.skip(1);
    return bytes.bytes[pos];
  }

  protected void serializeByte(BytesContainer bytes, byte value) {
    var pointer = bytes.alloc(1);
    bytes.bytes[pointer] = value;
  }

  public void serializeValue(
      DatabaseSessionInternal session, final BytesContainer bytes, Object value,
      final PropertyType type,
      final PropertyType linkedType) {
    var pointer = 0;
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
        LongSerializer.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        var fg = Float.floatToIntBits((Float) value);
        pointer = bytes.alloc(IntegerSerializer.INT_SIZE);
        IntegerSerializer.serializeLiteral(fg, bytes.bytes, pointer);
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
                DateHelper.getDatabaseTimeZone(session), TimeZone.getTimeZone("GMT"), dateValue);
        VarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        if (value instanceof EntitySerializable) {
          var cur = ((EntitySerializable) value).toEntity(session);
          cur.field(EntitySerializable.CLASS_NAME, value.getClass().getName());
          serialize(session, cur, bytes);
        } else {
          serialize(session, ((DBRecord) value).getRecord(session), bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray()) {
          writeEmbeddedCollection(session, bytes, Arrays.asList(MultiValue.array(value)),
              linkedType);
        } else {
          writeEmbeddedCollection(session, bytes, (Collection<?>) value, linkedType);
        }
        break;
      case DECIMAL:
        var decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(
            DecimalSerializer.INSTANCE.getObjectSize(session.getSerializerFactory(), decimalValue));
        DecimalSerializer.INSTANCE.serialize(decimalValue, session.getSerializerFactory(),
            bytes.bytes, pointer);
        break;
      case BINARY:
        pointer = writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        var ridCollection = (Collection<Identifiable>) value;
        writeLinkCollection(session, bytes, ridCollection);
        break;
      case LINK:
        if (!(value instanceof Identifiable)) {
          throw new ValidationException(session.getDatabaseName(),
              "Value '" + value + "' is not a Identifiable");
        }

        writeOptimizedLink(session, bytes, (Identifiable) value);
        break;
      case LINKMAP:
        writeLinkMap(session, bytes, (Map<Object, Identifiable>) value);
        break;
      case EMBEDDEDMAP:
        writeEmbeddedMap(session, bytes, (Map<Object, Object>) value);
        break;
      case LINKBAG:
        writeRidBag(session, bytes, (RidBag) value);
        break;
      case ANY:
        break;
    }
  }

  private static void writeLinkCollection(
      DatabaseSessionInternal session, final BytesContainer bytes,
      final Collection<Identifiable> value) {
    final var pos = VarIntSerializer.write(bytes, value.size());

    for (var itemValue : value) {
      // TODO: handle the null links
      if (itemValue == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(session, bytes, itemValue);
      }
    }

  }

  private static void writeLinkMap(DatabaseSessionInternal session, final BytesContainer bytes,
      final Map<Object, Identifiable> map) {
    final var fullPos = VarIntSerializer.write(bytes, map.size());
    for (var entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      final var type = PropertyType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(session, bytes, entry.getValue());
      }
    }
  }

  private void writeEmbeddedCollection(
      DatabaseSessionInternal session, final BytesContainer bytes, final Collection<?> value,
      final PropertyType linkedType) {
    VarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    for (var itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeNullableType(bytes, null);
        continue;
      }
      PropertyType type;
      if (linkedType == null) {
        type = getTypeFromValueEmbedded(itemValue);
      } else {
        type = linkedType;
      }
      if (type != null) {
        writeNullableType(bytes, type);
        serializeValue(session, bytes, itemValue, type, null);
      } else {
        throw new SerializationException(session.getDatabaseName(),
            "Impossible serialize value of type "
                + value.getClass()
                + " with the EntityImpl binary serializer");
      }
    }
  }

  private void writeEmbeddedMap(DatabaseSessionInternal session, BytesContainer bytes,
      Map<Object, Object> map) {
    VarIntSerializer.write(bytes, map.size());
    for (var entry : map.entrySet()) {
      writeString(bytes, entry.getKey().toString());
      final var value = entry.getValue();
      if (value != null) {
        final var type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new SerializationException(session.getDatabaseName(),
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the Result binary serializer");
        }
        writeNullableType(bytes, type);
        serializeValue(session, bytes, value, type, null);
      } else {
        writeNullableType(bytes, null);
      }
    }
  }

  public Object deserializeValue(DatabaseSessionInternal session, BytesContainer bytes,
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
                TimeZone.getTimeZone("GMT"), DateHelper.getDatabaseTimeZone(session), savedTime);
        value = new Date(savedTime);
        break;
      case EMBEDDED:
        value = new EmbeddedEntityImpl(session);
        deserialize(session, (EntityImpl) value, bytes);
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
          ((EntityImpl) value).addOwner(owner);
        }

        break;
      case EMBEDDEDSET:
        value = readEmbeddedSet(session, bytes, owner);

        break;
      case EMBEDDEDLIST:
        value = readEmbeddedList(session, bytes, owner);
        break;
      case LINKSET:
        value = readLinkSet(session, bytes, owner);
        break;
      case LINKLIST:
        value = readLinkList(session, bytes, owner);
        break;
      case BINARY:
        value = readBinary(bytes);
        break;
      case LINK:
        value = readOptimizedLink(session, bytes);
        break;
      case LINKMAP:
        value = readLinkMap(session, bytes, owner);
        break;
      case EMBEDDEDMAP:
        value = readEmbeddedMap(session, bytes, owner);
        break;
      case DECIMAL:
        value = DecimalSerializer.INSTANCE.deserialize(session.getSerializerFactory(), bytes.bytes,
            bytes.offset);
        bytes.skip(
            DecimalSerializer.INSTANCE.getObjectSize(session.getSerializerFactory(), bytes.bytes,
                bytes.offset));
        break;
      case LINKBAG:
        var bag = readRidBag(session, bytes);
        bag.setOwner(owner);
        value = bag;
        break;
      case ANY:
        break;
    }
    return value;
  }

  private Collection<?> readEmbeddedList(DatabaseSessionInternal session,
      final BytesContainer bytes, final RecordElement owner) {
    var found = new TrackedList<Object>(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      var itemType = readNullableType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(session, bytes, itemType, found));
      }
    }
    return found;
  }

  private TrackedSet<?> readEmbeddedSet(DatabaseSessionInternal session,
      final BytesContainer bytes, final RecordElement owner) {
    var found = new TrackedSet<>(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      var itemType = readNullableType(bytes);
      if (itemType == null) {
        found.addInternal(null);
      } else {
        found.addInternal(deserializeValue(session, bytes, itemType, found));
      }
    }
    return found;
  }

  private static Collection<Identifiable> readLinkList(DatabaseSessionInternal session,
      BytesContainer bytes,
      RecordElement owner) {
    var found = new LinkList(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      Identifiable id = readOptimizedLink(session, bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  private static Collection<Identifiable> readLinkSet(DatabaseSessionInternal session,
      BytesContainer bytes,
      RecordElement owner) {
    var found = new LinkSet(owner);
    final var items = VarIntSerializer.readAsInteger(bytes);
    for (var i = 0; i < items; i++) {
      Identifiable id = readOptimizedLink(session, bytes);
      if (id.equals(NULL_RECORD_ID)) {
        found.addInternal(null);
      } else {
        found.addInternal(id);
      }
    }
    return found;
  }

  private Map<String, Identifiable> readLinkMap(
      DatabaseSessionInternal session, final BytesContainer bytes, final RecordElement owner) {
    var size = VarIntSerializer.readAsInteger(bytes);
    var result = new LinkMap(owner);
    while ((size--) > 0) {
      var keyType = readOType(bytes, false);
      var key = deserializeValue(session, bytes, keyType, result);
      Identifiable value = readOptimizedLink(session, bytes);
      if (value.equals(NULL_RECORD_ID)) {
        result.putInternal(key.toString(), null);
      } else {
        result.putInternal(key.toString(), value);
      }
    }

    return result;
  }

  private Object readEmbeddedMap(DatabaseSessionInternal session, final BytesContainer bytes,
      final RecordElement owner) {
    var size = VarIntSerializer.readAsInteger(bytes);
    final TrackedMap result = new TrackedMap<Object>(owner);
    while ((size--) > 0) {
      var key = readString(bytes);
      var valType = readNullableType(bytes);
      Object value = null;
      if (valType != null) {
        value = deserializeValue(session, bytes, valType, result);
      }
      result.putInternal(key, value);
    }
    return result;
  }

  private RidBag readRidBag(DatabaseSessionInternal session, BytesContainer bytes) {
    var uuid = UUIDSerializer.INSTANCE.deserialize(session.getSerializerFactory(), bytes.bytes,
        bytes.offset);
    bytes.skip(UUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) {
      uuid = null;
    }
    var b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      var bag = new RidBag(session, uuid);
      var size = VarIntSerializer.readAsInteger(bytes);
      for (var i = 0; i < size; i++) {
        var id = readOptimizedLink(session, bytes);
        if (id.equals(NULL_RECORD_ID)) {
          bag.add(null);
        } else {
          bag.add(id);
        }
      }
      return bag;
    } else {
      var fileId = VarIntSerializer.readAsLong(bytes);
      var pageIndex = VarIntSerializer.readAsLong(bytes);
      var pageOffset = VarIntSerializer.readAsInteger(bytes);
      var bagSize = VarIntSerializer.readAsInteger(bytes);

      Map<RID, Change> changes = new HashMap<>();
      var size = VarIntSerializer.readAsInteger(bytes);
      while (size-- > 0) {
        RID link = readOptimizedLink(session, bytes);
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
      return new RidBag(session, pointer, changes, uuid);
    }
  }

  private static void writeRidBag(DatabaseSessionInternal session, BytesContainer bytes,
      RidBag bag) {
    final var bTreeCollectionManager = session.getSbTreeCollectionManager();
    UUID uuid = null;
    if (bTreeCollectionManager != null) {
      uuid = bTreeCollectionManager.listenForChanges(bag, session);
    }
    if (uuid == null) {
      uuid = new UUID(-1, -1);
    }
    var uuidPos = bytes.alloc(UUIDSerializer.UUID_SIZE);
    UUIDSerializer.INSTANCE.serialize(uuid, session.getSerializerFactory(), bytes.bytes, uuidPos);
    if (bag.isToSerializeEmbedded()) {
      var pos = bytes.alloc(1);
      bytes.bytes[pos] = 1;
      VarIntSerializer.write(bytes, bag.size());
      for (Identifiable itemValue : bag) {
        if (itemValue == null) {
          writeNullLink(bytes);
        } else {
          writeOptimizedLink(session, bytes, itemValue);
        }
      }
    } else {
      var pos = bytes.alloc(1);
      bytes.bytes[pos] = 2;
      var pointer = bag.getPointer();
      if (pointer == null) {
        pointer = BonsaiCollectionPointer.INVALID;
      }
      VarIntSerializer.write(bytes, pointer.getFileId());
      VarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
      VarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
      VarIntSerializer.write(bytes, bag.size());
      var changes = bag.getChanges();
      if (changes != null) {
        VarIntSerializer.write(bytes, changes.size());
        for (var change : changes.entrySet()) {
          writeOptimizedLink(session, bytes, change.getKey());
          var posAll = bytes.alloc(1);
          bytes.bytes[posAll] = change.getValue().getType();
          VarIntSerializer.write(bytes, change.getValue().getValue());
        }
      } else {
        VarIntSerializer.write(bytes, 0);
      }
    }
  }

  public static void writeNullableType(BytesContainer bytes, PropertyType type) {
    var pos = bytes.alloc(1);
    if (type == null) {
      bytes.bytes[pos] = -1;
    } else {
      bytes.bytes[pos] = (byte) type.getId();
    }
  }

  public static PropertyType readNullableType(BytesContainer bytes) {
    var typeId = bytes.bytes[bytes.offset++];
    if (typeId == -1) {
      return null;
    }
    return PropertyType.getById(typeId);
  }

  public static RID readOptimizedLink(DatabaseSessionInternal session, final BytesContainer bytes) {
    var clusterId = VarIntSerializer.readAsInteger(bytes);
    var clusterPos = VarIntSerializer.readAsLong(bytes);

    if (clusterId == -2 && clusterPos == -2) {
      return null;
    } else {
      RID rid = new RecordId(clusterId, clusterPos);

      if (rid.isTemporary()) {
        rid = session.refreshRid(rid);
      }

      return rid;
    }
  }

  public static void writeOptimizedLink(DatabaseSessionInternal session, final BytesContainer bytes,
      Identifiable link) {
    if (link == null) {
      VarIntSerializer.write(bytes, -2);
      VarIntSerializer.write(bytes, -2);
    } else {
      var rid = link.getIdentity();
      if (!rid.isPersistent()) {
        rid = session.refreshRid(rid);
      }

      VarIntSerializer.write(bytes, link.getIdentity().getClusterId());
      VarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    }
  }
}
