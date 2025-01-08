package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UUIDSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.RemoteTreeRidBag;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RecordSerializerNetworkV37Client extends RecordSerializerNetworkV37 {

  public static final RecordSerializerNetworkV37Client INSTANCE =
      new RecordSerializerNetworkV37Client();
  public static final String NAME = "onet_ser_v37_client";

  protected RidBag readRidBag(DatabaseSessionInternal db, BytesContainer bytes) {
    UUID uuid = UUIDSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(UUIDSerializer.UUID_SIZE);
    if (uuid.getMostSignificantBits() == -1 && uuid.getLeastSignificantBits() == -1) {
      uuid = null;
    }
    byte b = bytes.bytes[bytes.offset];
    bytes.skip(1);
    if (b == 1) {
      RidBag bag = new RidBag(db, uuid);
      int size = VarIntSerializer.readAsInteger(bytes);

      if (size > 0) {
        for (int i = 0; i < size; i++) {
          Identifiable id = readOptimizedLink(db, bytes);
          if (id.equals(NULL_RECORD_ID)) {
            bag.add(null);
          } else {
            bag.add(id.getIdentity());
          }
        }
        // The bag will mark the elements we just added as new events
        // and marking the entire bag as a dirty transaction {@link
        // EmbeddedRidBag#transactionDirty}
        // although we just deserialized it so there are no changes and the transaction isn't dirty
        bag.enableTracking(null);
        bag.transactionClear();
      }
      return bag;
    } else {
      long fileId = VarIntSerializer.readAsLong(bytes);
      long pageIndex = VarIntSerializer.readAsLong(bytes);
      int pageOffset = VarIntSerializer.readAsInteger(bytes);
      int bagSize = VarIntSerializer.readAsInteger(bytes);

      Map<Identifiable, Change> changes = new HashMap<>();
      int size = VarIntSerializer.readAsInteger(bytes);
      while (size-- > 0) {
        Identifiable link = readOptimizedLink(db, bytes);
        byte type = bytes.bytes[bytes.offset];
        bytes.skip(1);
        int change = VarIntSerializer.readAsInteger(bytes);
        changes.put(link, ChangeSerializationHelper.createChangeInstance(type, change));
      }
      BonsaiCollectionPointer pointer = null;
      if (fileId != -1) {
        pointer =
            new BonsaiCollectionPointer(fileId, new RidBagBucketPointer(pageIndex, pageOffset));
      }
      return new RidBag(db, new RemoteTreeRidBag(pointer));
    }
  }
}
