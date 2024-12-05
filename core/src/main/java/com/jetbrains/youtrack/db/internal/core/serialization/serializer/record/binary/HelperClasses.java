/*
 * Copyright 2018 YouTrackDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OLongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.OSharedContext;
import com.jetbrains.youtrack.db.internal.core.db.OStringCache;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.OTrackedMultiValue;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBagDelegate;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.embedded.EmbeddedRidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.OGlobalProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeRidBag;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionAbstract;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionOptimistic;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 *
 */
public class HelperClasses {

  public static final String CHARSET_UTF_8 = "UTF-8";
  protected static final YTRecordId NULL_RECORD_ID = new YTRecordId(-2, YTRID.CLUSTER_POS_INVALID);
  public static final long MILLISEC_PER_DAY = 86400000;

  public static class Tuple<T1, T2> {

    private final T1 firstVal;
    private final T2 secondVal;

    Tuple(T1 firstVal, T2 secondVal) {
      this.firstVal = firstVal;
      this.secondVal = secondVal;
    }

    public T1 getFirstVal() {
      return firstVal;
    }

    public T2 getSecondVal() {
      return secondVal;
    }
  }

  protected static class RecordInfo {

    public int fieldStartOffset;
    public int fieldLength;
    public YTType fieldType;
  }

  protected static class MapRecordInfo extends RecordInfo {

    public String key;
    public YTType keyType;
  }

  public static YTType readOType(final BytesContainer bytes, boolean justRunThrough) {
    if (justRunThrough) {
      bytes.offset++;
      return null;
    }
    return YTType.getById(readByte(bytes));
  }

  public static void writeOType(BytesContainer bytes, int pos, YTType type) {
    bytes.bytes[pos] = (byte) type.getId();
  }

  public static void writeType(BytesContainer bytes, YTType type) {
    int pos = bytes.alloc(1);
    bytes.bytes[pos] = (byte) type.getId();
  }

  public static YTType readType(BytesContainer bytes) {
    byte typeId = bytes.bytes[bytes.offset++];
    if (typeId == -1) {
      return null;
    }
    return YTType.getById(typeId);
  }

  public static byte[] readBinary(final BytesContainer bytes) {
    final int n = OVarIntSerializer.readAsInteger(bytes);
    final byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  public static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    if (len == 0) {
      return "";
    }
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  public static int readInteger(final BytesContainer container) {
    final int value =
        OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  public static byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  public static long readLong(final BytesContainer container) {
    final long value =
        OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  public static YTRecordId readOptimizedLink(final BytesContainer bytes, boolean justRunThrough) {
    int clusterId = OVarIntSerializer.readAsInteger(bytes);
    long clusterPos = OVarIntSerializer.readAsLong(bytes);
    if (justRunThrough) {
      return null;
    } else {
      return new YTRecordId(clusterId, clusterPos);
    }
  }

  public static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    return new String(bytes, offset, len, StandardCharsets.UTF_8);
  }

  public static String stringFromBytesIntern(final byte[] bytes, final int offset, final int len) {
    try {
      YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null) {
        OSharedContext context = db.getSharedContext();
        if (context != null) {
          OStringCache cache = context.getStringCache();
          if (cache != null) {
            return cache.getString(bytes, offset, len);
          }
        }
      }
      return new String(bytes, offset, len, StandardCharsets.UTF_8).intern();
    } catch (UnsupportedEncodingException e) {
      throw YTException.wrapException(new YTSerializationException("Error on string decoding"), e);
    }
  }

  public static byte[] bytesFromString(final String toWrite) {
    return toWrite.getBytes(StandardCharsets.UTF_8);
  }

  public static long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
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

  public static OGlobalProperty getGlobalProperty(final EntityImpl document, final int len) {
    final int id = (len * -1) - 1;
    return ODocumentInternal.getGlobalPropertyById(document, id);
  }

  public static int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  public static int writeOptimizedLink(final BytesContainer bytes, YTIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord();
      } catch (YTRecordNotFoundException ignored) {
        // IGNORE IT WILL FAIL THE ASSERT IN CASE
      }
    }
    if (link.getIdentity().getClusterId() < 0) {
      throw new YTDatabaseException("Impossible to serialize invalid link " + link.getIdentity());
    }

    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  public static int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;
  }

  public static YTType getTypeFromValueEmbedded(final Object fieldValue) {
    YTType type = YTType.getTypeByValue(fieldValue);
    if (type == YTType.LINK
        && fieldValue instanceof EntityImpl
        && !((EntityImpl) fieldValue).getIdentity().isValid()) {
      type = YTType.EMBEDDED;
    }
    return type;
  }

  public static int writeLinkCollection(
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

  public static <T extends OTrackedMultiValue<?, YTIdentifiable>> T readLinkCollection(
      final BytesContainer bytes, final T found, boolean justRunThrough) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      YTRecordId id = readOptimizedLink(bytes, justRunThrough);
      if (!justRunThrough) {
        if (id.equals(NULL_RECORD_ID)) {
          found.addInternal(null);
        } else {
          found.addInternal(id);
        }
      }
    }
    return found;
  }

  public static int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  public static int writeLinkMap(final BytesContainer bytes,
      final Map<Object, YTIdentifiable> map) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Map.Entry<Object, YTIdentifiable> entry : map.entrySet()) {
      writeString(bytes, entry.getKey().toString());
      if (entry.getValue() == null) {
        writeNullLink(bytes);
      } else {
        writeOptimizedLink(bytes, entry.getValue());
      }
    }
    return fullPos;
  }

  public static Map<Object, YTIdentifiable> readLinkMap(
      final BytesContainer bytes, final RecordElement owner, boolean justRunThrough) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    LinkMap result = null;
    if (!justRunThrough) {
      result = new LinkMap(owner);
    }
    while ((size--) > 0) {
      final String key = readString(bytes);
      final YTRecordId value = readOptimizedLink(bytes, justRunThrough);
      if (value.equals(NULL_RECORD_ID)) {
        result.putInternal(key, null);
      } else {
        result.putInternal(key, value);
      }
    }
    return result;
  }

  public static void writeByte(BytesContainer bytes, byte val) {
    int pos = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(val, bytes.bytes, pos);
  }

  public static void writeRidBag(BytesContainer bytes, RidBag ridbag) {
    ridbag.checkAndConvert();

    UUID ownerUuid = ridbag.getTemporaryId();

    int positionOffset = bytes.offset;
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    UUID uuid = null;
    if (sbTreeCollectionManager != null) {
      uuid = sbTreeCollectionManager.listenForChanges(ridbag);
    }

    byte configByte = 0;
    if (ridbag.isEmbedded()) {
      configByte |= 1;
    }

    if (uuid != null) {
      configByte |= 2;
    }

    // alloc will move offset and do skip
    int posForWrite = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(configByte, bytes.bytes, posForWrite);

    // removed serializing UUID

    if (ridbag.isEmbedded()) {
      writeEmbeddedRidbag(bytes, ridbag);
    } else {
      writeSBTreeRidbag(bytes, ridbag, ownerUuid);
    }
  }

  protected static void writeEmbeddedRidbag(BytesContainer bytes, RidBag ridbag) {
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    int size = ridbag.size();
    Object[] entries = ((EmbeddedRidBag) ridbag.getDelegate()).getEntries();
    for (int i = 0; i < entries.length; i++) {
      Object entry = entries[i];
      if (entry instanceof YTIdentifiable itemValue) {
        if (db != null
            && !db.isClosed()
            && db.getTransaction().isActive()
            && !itemValue.getIdentity().isPersistent()) {
          itemValue = db.getTransaction().getRecord(itemValue.getIdentity());
        }
        if (itemValue == null || itemValue == OTransactionAbstract.DELETED_RECORD) {
          entries[i] = null;
          // Decrease size, nulls are ignored
          size--;
        } else {
          entries[i] = itemValue.getIdentity();
        }
      }
    }

    OVarIntSerializer.write(bytes, size);
    for (int i = 0; i < entries.length; i++) {
      Object entry = entries[i];
      // Obviously this exclude nulls as well
      if (entry instanceof YTIdentifiable) {
        writeLinkOptimized(bytes, ((YTIdentifiable) entry).getIdentity());
      }
    }
  }

  protected static void writeSBTreeRidbag(BytesContainer bytes, RidBag ridbag, UUID ownerUuid) {
    ((OSBTreeRidBag) ridbag.getDelegate()).applyNewEntries();

    OBonsaiCollectionPointer pointer = ridbag.getPointer();

    final ORecordSerializationContext context;
    var db = ODatabaseRecordThreadLocal.instance().get();
    var tx = db.getTransaction();
    if (!(tx instanceof OTransactionOptimistic optimisticTx)) {
      throw new YTDatabaseException("Transaction is not active. Changes are not allowed");
    }

    boolean remoteMode = db.isRemote();

    if (remoteMode) {
      context = null;
    } else {
      context = ORecordSerializationContext.getContext();
    }

    if (pointer == null && context != null) {
      final int clusterId = getHighLevelDocClusterId(ridbag);
      assert clusterId > -1;
      try {
        final OAbstractPaginatedStorage storage =
            (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
        final OAtomicOperation atomicOperation =
            storage.getAtomicOperationsManager().getCurrentOperation();

        assert atomicOperation != null;
        pointer =
            ODatabaseRecordThreadLocal.instance()
                .get()
                .getSbTreeCollectionManager()
                .createSBTree(clusterId, atomicOperation, ownerUuid);
      } catch (IOException e) {
        throw YTException.wrapException(
            new YTDatabaseException("Error during creation of ridbag"), e);
      }
    }

    ((OSBTreeRidBag) ridbag.getDelegate()).setCollectionPointer(pointer);

    OVarIntSerializer.write(bytes, pointer.getFileId());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
    OVarIntSerializer.write(bytes, 0);

    if (context != null) {
      ((OSBTreeRidBag) ridbag.getDelegate()).handleContextSBTree(context, pointer);
      OVarIntSerializer.write(bytes, 0);
    } else {
      OVarIntSerializer.write(bytes, 0);

      // removed changes serialization
    }
  }

  private static int getHighLevelDocClusterId(RidBag ridbag) {
    RidBagDelegate delegate = ridbag.getDelegate();
    RecordElement owner = delegate.getOwner();
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null) {
      return ((YTIdentifiable) owner).getIdentity().getClusterId();
    }

    return -1;
  }

  public static void writeLinkOptimized(final BytesContainer bytes, YTIdentifiable link) {
    YTRID id = link.getIdentity();
    OVarIntSerializer.write(bytes, id.getClusterId());
    OVarIntSerializer.write(bytes, id.getClusterPosition());
  }

  public static RidBag readRidbag(YTDatabaseSessionInternal session, BytesContainer bytes) {
    byte configByte = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset++);
    boolean isEmbedded = (configByte & 1) != 0;

    UUID uuid = null;
    // removed deserializing UUID

    RidBag ridbag = null;
    if (isEmbedded) {
      ridbag = new RidBag(session);
      int size = OVarIntSerializer.readAsInteger(bytes);
      ridbag.getDelegate().setSize(size);
      for (int i = 0; i < size; i++) {
        YTIdentifiable record = readLinkOptimizedEmbedded(bytes);
        ridbag.getDelegate().addInternal(record);
      }
    } else {
      long fileId = OVarIntSerializer.readAsLong(bytes);
      long pageIndex = OVarIntSerializer.readAsLong(bytes);
      int pageOffset = OVarIntSerializer.readAsInteger(bytes);
      // read bag size
      OVarIntSerializer.readAsInteger(bytes);

      OBonsaiCollectionPointer pointer = null;
      if (fileId != -1) {
        pointer =
            new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
      }

      Map<YTIdentifiable, Change> changes = new HashMap<>();

      int changesSize = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < changesSize; i++) {
        YTIdentifiable recId = readLinkOptimizedSBTree(bytes);
        Change change = deserializeChange(bytes);
        changes.put(recId, change);
      }

      ridbag = new RidBag(session, pointer, changes, uuid);
      ridbag.getDelegate().setSize(-1);
    }
    return ridbag;
  }

  private static YTIdentifiable readLinkOptimizedEmbedded(final BytesContainer bytes) {
    YTRID rid =
        new YTRecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    YTIdentifiable identifiable = null;
    if (rid.isTemporary()) {
      try {
        identifiable = rid.getRecord();
      } catch (YTRecordNotFoundException rnf) {
        identifiable = rid;
      }
    }

    if (identifiable == null) {
      identifiable = rid;
    }

    return identifiable;
  }

  private static YTIdentifiable readLinkOptimizedSBTree(final BytesContainer bytes) {
    YTRID rid =
        new YTRecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    YTIdentifiable identifiable;
    if (rid.isTemporary()) {
      try {
        identifiable = rid.getRecord();
      } catch (YTRecordNotFoundException rnf) {
        identifiable = rid;
      }
    } else {
      identifiable = rid;
    }
    return identifiable;
  }

  private static Change deserializeChange(BytesContainer bytes) {
    byte type = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OByteSerializer.BYTE_SIZE);
    int change = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OIntegerSerializer.INT_SIZE);
    return ChangeSerializationHelper.createChangeInstance(type, change);
  }

  public static YTType getLinkedType(YTClass clazz, YTType type, String key) {
    if (type != YTType.EMBEDDEDLIST && type != YTType.EMBEDDEDSET && type != YTType.EMBEDDEDMAP) {
      return null;
    }
    if (clazz != null) {
      YTProperty prop = clazz.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }
}
