package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.client.remote.CollectionNetworkSerializer;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.IndexChange;
import com.jetbrains.youtrack.db.internal.client.remote.message.tx.RecordOperationRequest;
import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37Client;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.result.binary.ResultSerializerNetwork;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChangesPerKey;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChangesPerKey.TransactionIndexEntry;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.Nullable;

public class MessageHelper {

  public static void writeIdentifiable(
      DatabaseSessionInternal session, ChannelDataOutput channel, final Identifiable o,
      RecordSerializer serializer)
      throws IOException {
    if (o == null) {
      channel.writeShort(ChannelBinaryProtocol.RECORD_NULL);
    } else if (o instanceof RecordId) {
      channel.writeShort(ChannelBinaryProtocol.RECORD_RID);
      channel.writeRID((RID) o);
    } else {
      writeRecord(session, channel, o.getRecord(), serializer);
    }
  }

  public static void writeRecord(
      DatabaseSessionInternal session, ChannelDataOutput channel, RecordAbstract iRecord,
      RecordSerializer serializer)
      throws IOException {
    channel.writeShort((short) 0);
    channel.writeByte(RecordInternal.getRecordType(iRecord));
    channel.writeRID(iRecord.getIdentity());
    channel.writeVersion(iRecord.getVersion());
    try {
      final byte[] stream = getRecordBytes(session, iRecord, serializer);
      channel.writeBytes(stream);
    } catch (Exception e) {
      channel.writeBytes(null);
      final String message =
          "Error on marshalling record " + iRecord.getIdentity().toString() + " (" + e + ")";

      throw BaseException.wrapException(new SerializationException(message), e);
    }
  }

  public static byte[] getRecordBytes(@Nullable DatabaseSessionInternal session,
      final RecordAbstract iRecord, RecordSerializer serializer) {
    final byte[] stream;
    String dbSerializerName = null;
    if (session != null) {
      dbSerializerName = (iRecord.getSession()).getSerializer().toString();
    }
    if (RecordInternal.getRecordType(iRecord) == EntityImpl.RECORD_TYPE
        && (dbSerializerName == null || !dbSerializerName.equals(serializer.toString()))) {
      ((EntityImpl) iRecord).deserializeFields();
      stream = serializer.toStream(session, iRecord);
    } else {
      stream = iRecord.toStream();
    }

    return stream;
  }

  public static Map<UUID, BonsaiCollectionPointer> readCollectionChanges(ChannelDataInput network)
      throws IOException {
    Map<UUID, BonsaiCollectionPointer> collectionsUpdates = new HashMap<>();
    int count = network.readInt();
    for (int i = 0; i < count; i++) {
      final long mBitsOfId = network.readLong();
      final long lBitsOfId = network.readLong();

      final BonsaiCollectionPointer pointer =
          CollectionNetworkSerializer.INSTANCE.readCollectionPointer(network);

      collectionsUpdates.put(new UUID(mBitsOfId, lBitsOfId), pointer);
    }
    return collectionsUpdates;
  }

  public static void writeCollectionChanges(
      ChannelDataOutput channel, Map<UUID, BonsaiCollectionPointer> changedIds)
      throws IOException {
    channel.writeInt(changedIds.size());
    for (Entry<UUID, BonsaiCollectionPointer> entry : changedIds.entrySet()) {
      channel.writeLong(entry.getKey().getMostSignificantBits());
      channel.writeLong(entry.getKey().getLeastSignificantBits());
      CollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, entry.getValue());
    }
  }

  public static void writePhysicalPositions(
      ChannelDataOutput channel, PhysicalPosition[] previousPositions) throws IOException {
    if (previousPositions == null) {
      channel.writeInt(0); // NO ENTRIEs
    } else {
      channel.writeInt(previousPositions.length);

      for (final PhysicalPosition physicalPosition : previousPositions) {
        channel.writeLong(physicalPosition.clusterPosition);
        channel.writeInt(physicalPosition.recordSize);
        channel.writeVersion(physicalPosition.recordVersion);
      }
    }
  }

  public static PhysicalPosition[] readPhysicalPositions(ChannelDataInput network)
      throws IOException {
    final int positionsCount = network.readInt();
    final PhysicalPosition[] physicalPositions;
    if (positionsCount == 0) {
      physicalPositions = CommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
    } else {
      physicalPositions = new PhysicalPosition[positionsCount];

      for (int i = 0; i < physicalPositions.length; i++) {
        final PhysicalPosition position = new PhysicalPosition();

        position.clusterPosition = network.readLong();
        position.recordSize = network.readInt();
        position.recordVersion = network.readVersion();

        physicalPositions[i] = position;
      }
    }
    return physicalPositions;
  }

  public static RawPair<String[], int[]> readClustersArray(final ChannelDataInput network)
      throws IOException {
    final int tot = network.readShort();
    final String[] clusterNames = new String[tot];
    final int[] clusterIds = new int[tot];

    for (int i = 0; i < tot; ++i) {
      String clusterName = network.readString().toLowerCase(Locale.ENGLISH);
      final int clusterId = network.readShort();
      clusterNames[i] = clusterName;
      clusterIds[i] = clusterId;
    }

    return new RawPair<>(clusterNames, clusterIds);
  }

  public static void writeClustersArray(
      ChannelDataOutput channel, RawPair<String[], int[]> clusters, int protocolVersion)
      throws IOException {
    final String[] clusterNames = clusters.first;
    final int[] clusterIds = clusters.second;

    channel.writeShort((short) clusterNames.length);

    for (int i = 0; i < clusterNames.length; i++) {
      channel.writeString(clusterNames[i]);
      channel.writeShort((short) clusterIds[i]);
    }
  }

  public static void writeTransactionEntry(
      final DataOutput iNetwork, final RecordOperationRequest txEntry) throws IOException {
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeInt(txEntry.getId().getClusterId());
    iNetwork.writeLong(txEntry.getId().getClusterPosition());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case RecordOperation.CREATED:
        byte[] record = txEntry.getRecord();
        iNetwork.writeInt(record.length);
        iNetwork.write(record);
        break;

      case RecordOperation.UPDATED:
        iNetwork.writeInt(txEntry.getVersion());
        byte[] record2 = txEntry.getRecord();
        iNetwork.writeInt(record2.length);
        iNetwork.write(record2);
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case RecordOperation.DELETED:
        iNetwork.writeInt(txEntry.getVersion());
        break;
    }
  }

  static void writeTransactionEntry(
      final ChannelDataOutput iNetwork,
      final RecordOperationRequest txEntry,
      RecordSerializer serializer)
      throws IOException {
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case RecordOperation.CREATED:
        iNetwork.writeBytes(txEntry.getRecord());
        break;

      case RecordOperation.UPDATED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getRecord());
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case RecordOperation.DELETED:
        iNetwork.writeVersion(txEntry.getVersion());
        break;
    }
  }

  public static RecordOperationRequest readTransactionEntry(final DataInput iNetwork)
      throws IOException {
    RecordOperationRequest result = new RecordOperationRequest();
    result.setType(iNetwork.readByte());
    int clusterId = iNetwork.readInt();
    long clusterPosition = iNetwork.readLong();
    result.setId(new RecordId(clusterId, clusterPosition));
    result.setRecordType(iNetwork.readByte());

    switch (result.getType()) {
      case RecordOperation.CREATED:
        int length = iNetwork.readInt();
        byte[] record = new byte[length];
        iNetwork.readFully(record);
        result.setRecord(record);
        break;

      case RecordOperation.UPDATED:
        result.setVersion(iNetwork.readInt());
        int length2 = iNetwork.readInt();
        byte[] record2 = new byte[length2];
        iNetwork.readFully(record2);
        result.setRecord(record2);
        result.setContentChanged(iNetwork.readBoolean());
        break;

      case RecordOperation.DELETED:
        result.setVersion(iNetwork.readInt());
        break;
    }
    return result;
  }

  static RecordOperationRequest readTransactionEntry(
      ChannelDataInput channel, RecordSerializer ser) throws IOException {
    RecordOperationRequest entry = new RecordOperationRequest();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setRecordType(channel.readByte());
    switch (entry.getType()) {
      case RecordOperation.CREATED:
        entry.setRecord(channel.readBytes());
        break;
      case RecordOperation.UPDATED:
        entry.setVersion(channel.readVersion());
        entry.setRecord(channel.readBytes());
        entry.setContentChanged(channel.readBoolean());
        break;
      case RecordOperation.DELETED:
        entry.setVersion(channel.readVersion());
        break;
      default:
        break;
    }
    return entry;
  }

  static void writeTransactionIndexChanges(
      ChannelDataOutput network, RecordSerializerNetworkV37 serializer, List<IndexChange> changes)
      throws IOException {
    network.writeInt(changes.size());
    for (IndexChange indexChange : changes) {
      network.writeString(indexChange.getName());
      network.writeBoolean(indexChange.getKeyChanges().cleared);

      int size = indexChange.getKeyChanges().changesPerKey.size();
      if (indexChange.getKeyChanges().nullKeyChanges != null) {
        size += 1;
      }
      network.writeInt(size);
      if (indexChange.getKeyChanges().nullKeyChanges != null) {
        network.writeByte((byte) -1);
        network.writeInt(indexChange.getKeyChanges().nullKeyChanges.size());
        for (TransactionIndexEntry perKeyChange :
            indexChange.getKeyChanges().nullKeyChanges.getEntriesAsList()) {
          network.writeInt(perKeyChange.getOperation().ordinal());
          network.writeRID(perKeyChange.getValue().getIdentity());
        }
      }
      for (FrontendTransactionIndexChangesPerKey change :
          indexChange.getKeyChanges().changesPerKey.values()) {
        PropertyType type = PropertyType.getTypeByValue(change.key);
        byte[] value = serializer.serializeValue(change.key, type);
        network.writeByte((byte) type.getId());
        network.writeBytes(value);
        network.writeInt(change.size());
        for (TransactionIndexEntry perKeyChange :
            change.getEntriesAsList()) {
          FrontendTransactionIndexChanges.OPERATION op = perKeyChange.getOperation();
          if (op == FrontendTransactionIndexChanges.OPERATION.REMOVE
              && perKeyChange.getValue() == null) {
            op = FrontendTransactionIndexChanges.OPERATION.CLEAR;
          }

          network.writeInt(op.ordinal());
          if (op != FrontendTransactionIndexChanges.OPERATION.CLEAR) {
            network.writeRID(perKeyChange.getValue().getIdentity());
          }
        }
      }
    }
  }

  static List<IndexChange> readTransactionIndexChanges(
      DatabaseSessionInternal db, ChannelDataInput channel,
      RecordSerializerNetworkV37 serializer) throws IOException {
    List<IndexChange> changes = new ArrayList<>();
    int val = channel.readInt();
    while (val-- > 0) {
      String indexName = channel.readString();
      boolean cleared = channel.readBoolean();
      FrontendTransactionIndexChanges entry = new FrontendTransactionIndexChanges();
      entry.cleared = cleared;
      int changeCount = channel.readInt();
      NavigableMap<Object, FrontendTransactionIndexChangesPerKey> entries = new TreeMap<>();
      while (changeCount-- > 0) {
        byte bt = channel.readByte();
        Object key;
        if (bt == -1) {
          key = null;
        } else {
          PropertyType type = PropertyType.getById(bt);
          key = serializer.deserializeValue(db, channel.readBytes(), type);
        }
        FrontendTransactionIndexChangesPerKey changesPerKey = new FrontendTransactionIndexChangesPerKey(
            key);
        int keyChangeCount = channel.readInt();
        while (keyChangeCount-- > 0) {
          int op = channel.readInt();
          FrontendTransactionIndexChanges.OPERATION oper = FrontendTransactionIndexChanges.OPERATION.values()[op];
          RecordId id;
          if (oper == FrontendTransactionIndexChanges.OPERATION.CLEAR) {
            oper = FrontendTransactionIndexChanges.OPERATION.REMOVE;
            id = null;
          } else {
            id = channel.readRID();
          }
          changesPerKey.add(id, oper);
        }
        if (key == null) {
          entry.nullKeyChanges = changesPerKey;
        } else {
          entries.put(changesPerKey.key, changesPerKey);
        }
      }
      entry.changesPerKey = entries;
      changes.add(new IndexChange(indexName, entry));
    }
    return changes;
  }

  public static Identifiable readIdentifiable(
      DatabaseSessionInternal db, final ChannelDataInput network, RecordSerializer serializer)
      throws IOException {
    final int classId = network.readShort();
    if (classId == ChannelBinaryProtocol.RECORD_NULL) {
      return null;
    }

    if (classId == ChannelBinaryProtocol.RECORD_RID) {
      return network.readRID();
    } else {
      final Record record = readRecordFromBytes(db, network, serializer);
      return record;
    }
  }

  private static Record readRecordFromBytes(
      DatabaseSessionInternal db, ChannelDataInput network, RecordSerializer serializer)
      throws IOException {
    byte rec = network.readByte();
    final RecordId rid = network.readRID();
    final int version = network.readVersion();
    final byte[] content = network.readBytes();

    RecordAbstract record =
        YouTrackDBManager.instance()
            .getRecordFactoryManager()
            .newInstance(rec, rid, db);
    RecordInternal.setVersion(record, version);
    serializer.fromStream(db, content, record, null);
    RecordInternal.unsetDirty(record);

    return record;
  }

  private static void writeProjection(Result item, ChannelDataOutput channel)
      throws IOException {
    channel.writeByte(QueryResponse.RECORD_TYPE_PROJECTION);
    ResultSerializerNetwork ser = new ResultSerializerNetwork();
    ser.toStream(item, channel);
  }

  private static void writeBlob(
      DatabaseSessionInternal session, Result row, ChannelDataOutput channel,
      RecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(QueryResponse.RECORD_TYPE_BLOB);
    writeIdentifiable(session, channel, row.getBlob().get(), recordSerializer);
  }

  private static void writeVertex(
      DatabaseSessionInternal session, Result row, ChannelDataOutput channel,
      RecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(QueryResponse.RECORD_TYPE_VERTEX);
    writeDocument(session, channel, row.getEntity().get().getRecord(), recordSerializer);
  }

  private static void writeElement(
      DatabaseSessionInternal session, Result row, ChannelDataOutput channel,
      RecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(QueryResponse.RECORD_TYPE_ELEMENT);
    writeDocument(session, channel, row.getEntity().get().getRecord(), recordSerializer);
  }

  private static void writeEdge(
      DatabaseSessionInternal session, Result row, ChannelDataOutput channel,
      RecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(QueryResponse.RECORD_TYPE_EDGE);
    writeDocument(session, channel, row.getEntity().get().getRecord(), recordSerializer);
  }

  private static void writeDocument(
      DatabaseSessionInternal session, ChannelDataOutput channel, EntityImpl entity,
      RecordSerializer serializer) throws IOException {
    writeIdentifiable(session, channel, entity, serializer);
  }

  public static void writeResult(
      DatabaseSessionInternal session, Result row, ChannelDataOutput channel,
      RecordSerializer recordSerializer)
      throws IOException {
    if (row.isBlob()) {
      writeBlob(session, row, channel, recordSerializer);
    } else if (row.isVertex()) {
      writeVertex(session, row, channel, recordSerializer);
    } else if (row.isEdge()) {
      writeEdge(session, row, channel, recordSerializer);
    } else if (row.isEntity()) {
      writeElement(session, row, channel, recordSerializer);
    } else {
      writeProjection(row, channel);
    }
  }

  private static ResultInternal readBlob(DatabaseSessionInternal db, ChannelDataInput channel)
      throws IOException {
    RecordSerializer serializer = RecordSerializerNetworkV37.INSTANCE;
    return new ResultInternal(db, readIdentifiable(db, channel, serializer));
  }

  public static ResultInternal readResult(DatabaseSessionInternal db, ChannelDataInput channel)
      throws IOException {
    byte type = channel.readByte();
    return switch (type) {
      case QueryResponse.RECORD_TYPE_BLOB -> readBlob(db, channel);
      case QueryResponse.RECORD_TYPE_VERTEX -> readVertex(db, channel);
      case QueryResponse.RECORD_TYPE_EDGE -> readEdge(db, channel);
      case QueryResponse.RECORD_TYPE_ELEMENT -> readElement(db, channel);
      case QueryResponse.RECORD_TYPE_PROJECTION -> readProjection(db, channel);
      default -> new ResultInternal(db);
    };
  }

  private static ResultInternal readElement(DatabaseSessionInternal db,
      ChannelDataInput channel)
      throws IOException {
    return new ResultInternal(db, readDocument(db, channel));
  }

  private static ResultInternal readVertex(DatabaseSessionInternal db,
      ChannelDataInput channel)
      throws IOException {
    return new ResultInternal(db, readDocument(db, channel));
  }

  private static ResultInternal readEdge(DatabaseSessionInternal db, ChannelDataInput channel)
      throws IOException {
    return new ResultInternal(db, readDocument(db, channel));
  }

  private static Record readDocument(DatabaseSessionInternal db, ChannelDataInput channel)
      throws IOException {
    RecordSerializer serializer = RecordSerializerNetworkV37Client.INSTANCE;
    return (Record) readIdentifiable(db, channel, serializer);
  }

  private static ResultInternal readProjection(DatabaseSessionInternal db,
      ChannelDataInput channel) throws IOException {
    ResultSerializerNetwork ser = new ResultSerializerNetwork();
    return ser.fromStream(db, channel);
  }
}
