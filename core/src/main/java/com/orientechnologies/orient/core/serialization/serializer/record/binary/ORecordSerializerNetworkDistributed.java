package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Collection;
import java.util.Map;

public class ORecordSerializerNetworkDistributed extends ORecordSerializerNetworkV37 {

  public static final ORecordSerializerNetworkDistributed INSTANCE =
      new ORecordSerializerNetworkDistributed();

  protected void writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        link = link.getRecord();
      } catch (ORecordNotFoundException rnf) {
        // IGNORE IT
      }
    }

    if (!link.getIdentity().isPersistent() && !link.getIdentity().isTemporary()) {
      throw new ODatabaseException(
          "Found not persistent link with no connected record, probably missing save `"
              + link.getIdentity()
              + "` ");
    }
    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
  }

  protected Collection<Map.Entry<String, ODocumentEntry>> fetchEntries(ODocument document) {
    return ODocumentInternal.rawEntries(document);
  }
}
