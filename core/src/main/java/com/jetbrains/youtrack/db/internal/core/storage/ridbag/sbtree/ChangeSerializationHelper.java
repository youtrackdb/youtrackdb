package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ChangeSerializationHelper {

  public static final ChangeSerializationHelper INSTANCE = new ChangeSerializationHelper();

  public static Change createChangeInstance(byte type, int value) {
    switch (type) {
      case AbsoluteChange.TYPE:
        return new AbsoluteChange(value);
      case DiffChange.TYPE:
        return new DiffChange(value);
      default:
        throw new IllegalArgumentException("Change type is incorrect");
    }
  }

  public Change deserializeChange(final byte[] stream, final int offset) {
    int value =
        IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset + ByteSerializer.BYTE_SIZE);
    return createChangeInstance(ByteSerializer.INSTANCE.deserializeLiteral(stream, offset), value);
  }

  public Map<Identifiable, Change> deserializeChanges(final byte[] stream, int offset) {
    final int count = IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    final HashMap<Identifiable, Change> res = new HashMap<>();
    for (int i = 0; i < count; i++) {
      RecordId rid = LinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += LinkSerializer.RID_SIZE;
      Change change = ChangeSerializationHelper.INSTANCE.deserializeChange(stream, offset);
      offset += Change.SIZE;

      Identifiable identifiable;
      try {
        if (rid.isTemporary()) {
          identifiable = rid.getRecord();
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

  public <K extends Identifiable> void serializeChanges(
      Map<K, Change> changes, BinarySerializer<K> keySerializer, byte[] stream, int offset) {
    IntegerSerializer.INSTANCE.serializeLiteral(changes.size(), stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    for (Map.Entry<K, Change> entry : changes.entrySet()) {
      K key = entry.getKey();

      if (key.getIdentity().isTemporary())
      //noinspection unchecked
      {
        key = key.getRecord();
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
