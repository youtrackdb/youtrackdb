package com.jetbrains.youtrack.db.internal.core.storage.ridbag;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RID;
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

  public static Change deserializeChange(final byte[] stream, final int offset) {
    var value =
        IntegerSerializer.deserializeLiteral(stream, offset + ByteSerializer.BYTE_SIZE);
    return createChangeInstance(ByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  public static Map<RID, Change> deserializeChanges(DatabaseSessionInternal session,
      final byte[] stream, int offset) {
    final var count = IntegerSerializer.deserializeLiteral(stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    final var res = new HashMap<RID, Change>();
    for (var i = 0; i < count; i++) {
      var rid = LinkSerializer.staticDeserialize(stream, offset);
      offset += LinkSerializer.RID_SIZE;
      var change = ChangeSerializationHelper.deserializeChange(stream, offset);
      offset += Change.SIZE;

      RID identifiable;
      if (rid.isTemporary()) {
        identifiable = session.refreshRid(rid);
      } else {
        identifiable = rid;
      }

      res.put(identifiable, change);
    }

    return res;
  }

  public static void serializeChanges(
      DatabaseSessionInternal db, Map<RID, Change> changes, byte[] stream, int offset) {
    IntegerSerializer.serializeLiteral(changes.size(), stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    for (var entry : changes.entrySet()) {
      var rid = entry.getKey();
      if (rid.isTemporary()) {
        rid = db.refreshRid(rid);
      } else if (rid instanceof DBRecord record && record.getIdentity().isTemporary()) {
        rid = db.refreshRid(record.getIdentity());
      }

      LinkSerializer.staticSerialize(rid, stream, offset);
      offset += LinkSerializer.staticGetObjectSize();

      offset += entry.getValue().serialize(stream, offset);
    }
  }

  public static int getChangesSerializedSize(int changesCount) {
    return changesCount * (LinkSerializer.RID_SIZE + Change.SIZE);
  }
}
