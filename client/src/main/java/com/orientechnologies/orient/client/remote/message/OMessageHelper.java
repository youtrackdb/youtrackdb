package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTSerializationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.serialization.serializer.result.binary.OResultSerializerNetwork;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
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

public class OMessageHelper {

  public static void writeIdentifiable(
      YTDatabaseSessionInternal session, OChannelDataOutput channel, final YTIdentifiable o,
      ORecordSerializer serializer)
      throws IOException {
    if (o == null) {
      channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
    } else if (o instanceof YTRecordId) {
      channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
      channel.writeRID((YTRID) o);
    } else {
      writeRecord(session, channel, o.getRecord(), serializer);
    }
  }

  public static void writeRecord(
      YTDatabaseSessionInternal session, OChannelDataOutput channel, YTRecordAbstract iRecord,
      ORecordSerializer serializer)
      throws IOException {
    channel.writeShort((short) 0);
    channel.writeByte(ORecordInternal.getRecordType(iRecord));
    channel.writeRID(iRecord.getIdentity());
    channel.writeVersion(iRecord.getVersion());
    try {
      final byte[] stream = getRecordBytes(session, iRecord, serializer);
      channel.writeBytes(stream);
    } catch (Exception e) {
      channel.writeBytes(null);
      final String message =
          "Error on marshalling record " + iRecord.getIdentity().toString() + " (" + e + ")";

      throw YTException.wrapException(new YTSerializationException(message), e);
    }
  }

  public static byte[] getRecordBytes(@Nullable YTDatabaseSessionInternal session,
      final YTRecordAbstract iRecord, ORecordSerializer serializer) {
    final byte[] stream;
    String dbSerializerName = null;
    if (session != null) {
      dbSerializerName = (iRecord.getSession()).getSerializer().toString();
    }
    if (ORecordInternal.getRecordType(iRecord) == YTDocument.RECORD_TYPE
        && (dbSerializerName == null || !dbSerializerName.equals(serializer.toString()))) {
      ((YTDocument) iRecord).deserializeFields();
      stream = serializer.toStream(session, iRecord);
    } else {
      stream = iRecord.toStream();
    }

    return stream;
  }

  public static Map<UUID, OBonsaiCollectionPointer> readCollectionChanges(OChannelDataInput network)
      throws IOException {
    Map<UUID, OBonsaiCollectionPointer> collectionsUpdates = new HashMap<>();
    int count = network.readInt();
    for (int i = 0; i < count; i++) {
      final long mBitsOfId = network.readLong();
      final long lBitsOfId = network.readLong();

      final OBonsaiCollectionPointer pointer =
          OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(network);

      collectionsUpdates.put(new UUID(mBitsOfId, lBitsOfId), pointer);
    }
    return collectionsUpdates;
  }

  public static void writeCollectionChanges(
      OChannelDataOutput channel, Map<UUID, OBonsaiCollectionPointer> changedIds)
      throws IOException {
    channel.writeInt(changedIds.size());
    for (Entry<UUID, OBonsaiCollectionPointer> entry : changedIds.entrySet()) {
      channel.writeLong(entry.getKey().getMostSignificantBits());
      channel.writeLong(entry.getKey().getLeastSignificantBits());
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, entry.getValue());
    }
  }

  public static void writePhysicalPositions(
      OChannelDataOutput channel, OPhysicalPosition[] previousPositions) throws IOException {
    if (previousPositions == null) {
      channel.writeInt(0); // NO ENTRIEs
    } else {
      channel.writeInt(previousPositions.length);

      for (final OPhysicalPosition physicalPosition : previousPositions) {
        channel.writeLong(physicalPosition.clusterPosition);
        channel.writeInt(physicalPosition.recordSize);
        channel.writeVersion(physicalPosition.recordVersion);
      }
    }
  }

  public static OPhysicalPosition[] readPhysicalPositions(OChannelDataInput network)
      throws IOException {
    final int positionsCount = network.readInt();
    final OPhysicalPosition[] physicalPositions;
    if (positionsCount == 0) {
      physicalPositions = OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
    } else {
      physicalPositions = new OPhysicalPosition[positionsCount];

      for (int i = 0; i < physicalPositions.length; i++) {
        final OPhysicalPosition position = new OPhysicalPosition();

        position.clusterPosition = network.readLong();
        position.recordSize = network.readInt();
        position.recordVersion = network.readVersion();

        physicalPositions[i] = position;
      }
    }
    return physicalPositions;
  }

  public static ORawPair<String[], int[]> readClustersArray(final OChannelDataInput network)
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

    return new ORawPair<>(clusterNames, clusterIds);
  }

  public static void writeClustersArray(
      OChannelDataOutput channel, ORawPair<String[], int[]> clusters, int protocolVersion)
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
      final DataOutput iNetwork, final ORecordOperationRequest txEntry) throws IOException {
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeInt(txEntry.getId().getClusterId());
    iNetwork.writeLong(txEntry.getId().getClusterPosition());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case ORecordOperation.CREATED:
        byte[] record = txEntry.getRecord();
        iNetwork.writeInt(record.length);
        iNetwork.write(record);
        break;

      case ORecordOperation.UPDATED:
        iNetwork.writeInt(txEntry.getVersion());
        byte[] record2 = txEntry.getRecord();
        iNetwork.writeInt(record2.length);
        iNetwork.write(record2);
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case ORecordOperation.DELETED:
        iNetwork.writeInt(txEntry.getVersion());
        break;
    }
  }

  static void writeTransactionEntry(
      final OChannelDataOutput iNetwork,
      final ORecordOperationRequest txEntry,
      ORecordSerializer serializer)
      throws IOException {
    iNetwork.writeByte(txEntry.getType());
    iNetwork.writeRID(txEntry.getId());
    iNetwork.writeByte(txEntry.getRecordType());

    switch (txEntry.getType()) {
      case ORecordOperation.CREATED:
        iNetwork.writeBytes(txEntry.getRecord());
        break;

      case ORecordOperation.UPDATED:
        iNetwork.writeVersion(txEntry.getVersion());
        iNetwork.writeBytes(txEntry.getRecord());
        iNetwork.writeBoolean(txEntry.isContentChanged());
        break;

      case ORecordOperation.DELETED:
        iNetwork.writeVersion(txEntry.getVersion());
        break;
    }
  }

  public static ORecordOperationRequest readTransactionEntry(final DataInput iNetwork)
      throws IOException {
    ORecordOperationRequest result = new ORecordOperationRequest();
    result.setType(iNetwork.readByte());
    int clusterId = iNetwork.readInt();
    long clusterPosition = iNetwork.readLong();
    result.setId(new YTRecordId(clusterId, clusterPosition));
    result.setRecordType(iNetwork.readByte());

    switch (result.getType()) {
      case ORecordOperation.CREATED:
        int length = iNetwork.readInt();
        byte[] record = new byte[length];
        iNetwork.readFully(record);
        result.setRecord(record);
        break;

      case ORecordOperation.UPDATED:
        result.setVersion(iNetwork.readInt());
        int length2 = iNetwork.readInt();
        byte[] record2 = new byte[length2];
        iNetwork.readFully(record2);
        result.setRecord(record2);
        result.setContentChanged(iNetwork.readBoolean());
        break;

      case ORecordOperation.DELETED:
        result.setVersion(iNetwork.readInt());
        break;
    }
    return result;
  }

  static ORecordOperationRequest readTransactionEntry(
      OChannelDataInput channel, ORecordSerializer ser) throws IOException {
    ORecordOperationRequest entry = new ORecordOperationRequest();
    entry.setType(channel.readByte());
    entry.setId(channel.readRID());
    entry.setRecordType(channel.readByte());
    switch (entry.getType()) {
      case ORecordOperation.CREATED:
        entry.setRecord(channel.readBytes());
        break;
      case ORecordOperation.UPDATED:
        entry.setVersion(channel.readVersion());
        entry.setRecord(channel.readBytes());
        entry.setContentChanged(channel.readBoolean());
        break;
      case ORecordOperation.DELETED:
        entry.setVersion(channel.readVersion());
        break;
      default:
        break;
    }
    return entry;
  }

  static void writeTransactionIndexChanges(
      OChannelDataOutput network, ORecordSerializerNetworkV37 serializer, List<IndexChange> changes)
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
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry perKeyChange :
            indexChange.getKeyChanges().nullKeyChanges.getEntriesAsList()) {
          network.writeInt(perKeyChange.getOperation().ordinal());
          network.writeRID(perKeyChange.getValue().getIdentity());
        }
      }
      for (OTransactionIndexChangesPerKey change :
          indexChange.getKeyChanges().changesPerKey.values()) {
        YTType type = YTType.getTypeByValue(change.key);
        byte[] value = serializer.serializeValue(change.key, type);
        network.writeByte((byte) type.getId());
        network.writeBytes(value);
        network.writeInt(change.size());
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry perKeyChange :
            change.getEntriesAsList()) {
          OTransactionIndexChanges.OPERATION op = perKeyChange.getOperation();
          if (op == OTransactionIndexChanges.OPERATION.REMOVE && perKeyChange.getValue() == null) {
            op = OTransactionIndexChanges.OPERATION.CLEAR;
          }

          network.writeInt(op.ordinal());
          if (op != OTransactionIndexChanges.OPERATION.CLEAR) {
            network.writeRID(perKeyChange.getValue().getIdentity());
          }
        }
      }
    }
  }

  static List<IndexChange> readTransactionIndexChanges(
      YTDatabaseSessionInternal db, OChannelDataInput channel,
      ORecordSerializerNetworkV37 serializer) throws IOException {
    List<IndexChange> changes = new ArrayList<>();
    int val = channel.readInt();
    while (val-- > 0) {
      String indexName = channel.readString();
      boolean cleared = channel.readBoolean();
      OTransactionIndexChanges entry = new OTransactionIndexChanges();
      entry.cleared = cleared;
      int changeCount = channel.readInt();
      NavigableMap<Object, OTransactionIndexChangesPerKey> entries = new TreeMap<>();
      while (changeCount-- > 0) {
        byte bt = channel.readByte();
        Object key;
        if (bt == -1) {
          key = null;
        } else {
          YTType type = YTType.getById(bt);
          key = serializer.deserializeValue(db, channel.readBytes(), type);
        }
        OTransactionIndexChangesPerKey changesPerKey = new OTransactionIndexChangesPerKey(key);
        int keyChangeCount = channel.readInt();
        while (keyChangeCount-- > 0) {
          int op = channel.readInt();
          OTransactionIndexChanges.OPERATION oper = OTransactionIndexChanges.OPERATION.values()[op];
          YTRecordId id;
          if (oper == OTransactionIndexChanges.OPERATION.CLEAR) {
            oper = OTransactionIndexChanges.OPERATION.REMOVE;
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

  public static YTIdentifiable readIdentifiable(
      YTDatabaseSessionInternal db, final OChannelDataInput network, ORecordSerializer serializer)
      throws IOException {
    final int classId = network.readShort();
    if (classId == OChannelBinaryProtocol.RECORD_NULL) {
      return null;
    }

    if (classId == OChannelBinaryProtocol.RECORD_RID) {
      return network.readRID();
    } else {
      final YTRecord record = readRecordFromBytes(db, network, serializer);
      return record;
    }
  }

  private static YTRecord readRecordFromBytes(
      YTDatabaseSessionInternal db, OChannelDataInput network, ORecordSerializer serializer)
      throws IOException {
    byte rec = network.readByte();
    final YTRecordId rid = network.readRID();
    final int version = network.readVersion();
    final byte[] content = network.readBytes();

    YTRecordAbstract record =
        YouTrackDBManager.instance()
            .getRecordFactoryManager()
            .newInstance(rec, rid, db);
    ORecordInternal.setVersion(record, version);
    serializer.fromStream(db, content, record, null);
    ORecordInternal.unsetDirty(record);

    return record;
  }

  private static void writeProjection(OResult item, OChannelDataOutput channel) throws IOException {
    channel.writeByte(OQueryResponse.RECORD_TYPE_PROJECTION);
    OResultSerializerNetwork ser = new OResultSerializerNetwork();
    ser.toStream(item, channel);
  }

  private static void writeBlob(
      YTDatabaseSessionInternal session, OResult row, OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(OQueryResponse.RECORD_TYPE_BLOB);
    writeIdentifiable(session, channel, row.getBlob().get(), recordSerializer);
  }

  private static void writeVertex(
      YTDatabaseSessionInternal session, OResult row, OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(OQueryResponse.RECORD_TYPE_VERTEX);
    writeDocument(session, channel, row.getElement().get().getRecord(), recordSerializer);
  }

  private static void writeElement(
      YTDatabaseSessionInternal session, OResult row, OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(OQueryResponse.RECORD_TYPE_ELEMENT);
    writeDocument(session, channel, row.getElement().get().getRecord(), recordSerializer);
  }

  private static void writeEdge(
      YTDatabaseSessionInternal session, OResult row, OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    channel.writeByte(OQueryResponse.RECORD_TYPE_EDGE);
    writeDocument(session, channel, row.getElement().get().getRecord(), recordSerializer);
  }

  private static void writeDocument(
      YTDatabaseSessionInternal session, OChannelDataOutput channel, YTDocument doc,
      ORecordSerializer serializer) throws IOException {
    writeIdentifiable(session, channel, doc, serializer);
  }

  public static void writeResult(
      YTDatabaseSessionInternal session, OResult row, OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    if (row.isBlob()) {
      writeBlob(session, row, channel, recordSerializer);
    } else if (row.isVertex()) {
      writeVertex(session, row, channel, recordSerializer);
    } else if (row.isEdge()) {
      writeEdge(session, row, channel, recordSerializer);
    } else if (row.isElement()) {
      writeElement(session, row, channel, recordSerializer);
    } else {
      writeProjection(row, channel);
    }
  }

  private static OResultInternal readBlob(YTDatabaseSessionInternal db, OChannelDataInput channel)
      throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37.INSTANCE;
    return new OResultInternal(db, readIdentifiable(db, channel, serializer));
  }

  public static OResultInternal readResult(YTDatabaseSessionInternal db, OChannelDataInput channel)
      throws IOException {
    byte type = channel.readByte();
    return switch (type) {
      case OQueryResponse.RECORD_TYPE_BLOB -> readBlob(db, channel);
      case OQueryResponse.RECORD_TYPE_VERTEX -> readVertex(db, channel);
      case OQueryResponse.RECORD_TYPE_EDGE -> readEdge(db, channel);
      case OQueryResponse.RECORD_TYPE_ELEMENT -> readElement(db, channel);
      case OQueryResponse.RECORD_TYPE_PROJECTION -> readProjection(db, channel);
      default -> new OResultInternal(db);
    };
  }

  private static OResultInternal readElement(YTDatabaseSessionInternal db,
      OChannelDataInput channel)
      throws IOException {
    return new OResultInternal(db, readDocument(db, channel));
  }

  private static OResultInternal readVertex(YTDatabaseSessionInternal db, OChannelDataInput channel)
      throws IOException {
    return new OResultInternal(db, readDocument(db, channel));
  }

  private static OResultInternal readEdge(YTDatabaseSessionInternal db, OChannelDataInput channel)
      throws IOException {
    return new OResultInternal(db, readDocument(db, channel));
  }

  private static YTRecord readDocument(YTDatabaseSessionInternal db, OChannelDataInput channel)
      throws IOException {
    ORecordSerializer serializer = ORecordSerializerNetworkV37Client.INSTANCE;
    return (YTRecord) readIdentifiable(db, channel, serializer);
  }

  private static OResultInternal readProjection(YTDatabaseSessionInternal db,
      OChannelDataInput channel) throws IOException {
    OResultSerializerNetwork ser = new OResultSerializerNetwork();
    return ser.fromStream(db, channel);
  }
}
