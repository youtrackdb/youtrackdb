package com.orientechnologies.core.serialization.serializer.record.binary;

import com.orientechnologies.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.id.YTRecordId;

public class ODocumentSerializerDeltaDistributed extends ODocumentSerializerDelta {

  private static final ODocumentSerializerDeltaDistributed INSTANCE =
      new ODocumentSerializerDeltaDistributed();

  public static ODocumentSerializerDeltaDistributed instance() {
    return INSTANCE;
  }

  protected void deserializeDeltaLinkBag(BytesContainer bytes, RidBag toUpdate) {
    boolean isTree = deserializeByte(bytes) == 1;
    long rootChanges = OVarIntSerializer.readAsLong(bytes);
    while (rootChanges-- > 0) {
      byte change = deserializeByte(bytes);
      switch (change) {
        case CREATED: {
          YTRecordId link = readOptimizedLink(bytes);
          if (toUpdate != null) {
            toUpdate.add(link);
          }
          break;
        }
        case REPLACED: {
          break;
        }
        case REMOVED: {
          YTRecordId link = readOptimizedLink(bytes);
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

  protected void serializeDeltaLinkBag(BytesContainer bytes, RidBag value) {
    serializeByte(bytes, value.isEmbedded() ? (byte) 0 : 1);
    OMultiValueChangeTimeLine<YTIdentifiable, YTIdentifiable> timeline =
        value.getTransactionTimeLine();
    assert timeline != null : "Cx ollection timeline required for link types serialization";
    OVarIntSerializer.write(bytes, timeline.getMultiValueChangeEvents().size());
    for (OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable> event :
        timeline.getMultiValueChangeEvents()) {
      switch (event.getChangeType()) {
        case ADD:
          serializeByte(bytes, CREATED);
          writeOptimizedLink(bytes, event.getValue());
          break;
        case UPDATE:
          throw new UnsupportedOperationException(
              "update do not happen in sets, it will be like and add");
        case REMOVE:
          serializeByte(bytes, REMOVED);
          writeOptimizedLink(bytes, event.getOldValue());
          break;
      }
    }
  }
}
