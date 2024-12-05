package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.internal.common.serialization.types.OUUIDSerializer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.RemoteTreeRidBag;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ORecordSerializerNetworkV37Client extends ORecordSerializerNetworkV37 {

  public static final ORecordSerializerNetworkV37Client INSTANCE =
      new ORecordSerializerNetworkV37Client();
  public static final String NAME = "onet_ser_v37_client";

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
      int size = OVarIntSerializer.readAsInteger(bytes);

      if (size > 0) {
        for (int i = 0; i < size; i++) {
          YTIdentifiable id = readOptimizedLink(bytes);
          if (id.equals(NULL_RECORD_ID)) {
            bag.add(null);
          } else {
            bag.add(id);
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
      return new RidBag(db, new RemoteTreeRidBag(pointer));
    }
  }
}
