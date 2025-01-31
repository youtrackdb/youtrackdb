package com.jetbrains.youtrack.db.internal.core.storage.ridbag;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ChangeSerializationHelper {

  public static final ChangeSerializationHelper INSTANCE = new ChangeSerializationHelper();

  public static Change createChangeInstance(byte type, int value) {
    return switch (type) {
      case AbsoluteChange.TYPE -> new AbsoluteChange(value);
      case DiffChange.TYPE -> new DiffChange(value);
      default -> throw new IllegalArgumentException("Change type is incorrect");
    };
  }

  public Change deserializeChange(final byte[] stream, final int offset) {
    var value =
        IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + ByteSerializer.BYTE_SIZE);
    return createChangeInstance(ByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  public static Map<RID, Change> deserializeChanges(DatabaseSessionInternal db,
      final byte[] stream, int offset) {
    final var count = IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    final var res = new HashMap<RID, Change>();
    for (var i = 0; i < count; i++) {
      var rid = LinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += LinkSerializer.RID_SIZE;
      var change = ChangeSerializationHelper.INSTANCE.deserializeChange(stream, offset);
      offset += Change.SIZE;

      RID identifiable;
      try {
        if (rid.isTemporary()) {
          identifiable = rid.getRecord(db).getIdentity();
        } else {
          identifiable = rid;
        }
      } catch (RecordNotFoundException rnf) {
        identifiable = rid;
      }

      res.put(identifiable, change);
    }

    return res;
  }

  public static <K extends Identifiable> void serializeChanges(
      DatabaseSessionInternal db, Map<K, Change> changes, BinarySerializer<K> keySerializer,
      byte[] stream, int offset) {
    IntegerSerializer.INSTANCE.serializeLiteral(changes.size(), stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    for (var entry : changes.entrySet()) {
      var key = entry.getKey();
      if (key instanceof RID rid && rid.isTemporary()) {
        try {
          //noinspection unchecked
          key = (K) rid.getRecord(db).getIdentity();
        } catch (RecordNotFoundException e) {
          //ignore
        }
      } else if (key instanceof DBRecord record && record.getIdentity().isTemporary()) {
        try {
          //noinspection unchecked
          key = record.getIdentity().getRecord(db);
        } catch (RecordNotFoundException e) {
          //ignore
        }
      }

      keySerializer.serialize(key, stream, offset);
      offset += keySerializer.getObjectSize(key);

      offset += entry.getValue().serialize(stream, offset);
    }
  }

  public int getChangesSerializedSize(int changesCount) {
    return changesCount * (LinkSerializer.RID_SIZE + Change.SIZE);
  }
}
