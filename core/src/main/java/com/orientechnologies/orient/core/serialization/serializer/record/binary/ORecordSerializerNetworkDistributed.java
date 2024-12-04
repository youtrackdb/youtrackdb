package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.Collection;
import java.util.Map;

public class ORecordSerializerNetworkDistributed extends ORecordSerializerNetworkV37 {

  public static final ORecordSerializerNetworkDistributed INSTANCE =
      new ORecordSerializerNetworkDistributed();

  protected void writeOptimizedLink(final BytesContainer bytes, YTIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord();
      } catch (YTRecordNotFoundException rnf) {
        // IGNORE IT
      }
    }

    if (!link.getIdentity().isPersistent() && !link.getIdentity().isTemporary()) {
      throw new YTDatabaseException(
          "Found not persistent link with no connected record, probably missing save `"
              + link.getIdentity()
              + "` ");
    }
    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
  }

  protected Collection<Map.Entry<String, ODocumentEntry>> fetchEntries(YTDocument document) {
    return ODocumentInternal.rawEntries(document);
  }
}
