package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;

public class DocumentSerializerDeltaDistributed extends DocumentSerializerDelta {

  private static final DocumentSerializerDeltaDistributed INSTANCE =
      new DocumentSerializerDeltaDistributed();

  public static DocumentSerializerDeltaDistributed instance() {
    return INSTANCE;
  }

  protected void deserializeDeltaLinkBag(DatabaseSessionInternal db, BytesContainer bytes,
      RidBag toUpdate) {
    var isTree = deserializeByte(bytes) == 1;
    var rootChanges = VarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      var change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          var link = readOptimizedLink(db, bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          var link = readOptimizedLink(db, bytes);
          if (toUpdate != null) {
            toUpdate.remove(link);
          }
          break;
        }
      }
    }
    if (toUpdate != null) {
      if (isTree) {
        toUpdate.makeTree();
      } else {
        toUpdate.makeEmbedded();
      }
    }
  }

  protected void serializeDeltaLinkBag(DatabaseSessionInternal db, BytesContainer bytes,
      RidBag value) {
    serializeByte(bytes, value.isEmbedded() ? (byte) 0 : 1);
    var timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Cx ollection timeline required for link types serialization";
    VarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (var event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(db, bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(db, bytes, event.getOldValue());
          break;
      }
    }
  }
}
