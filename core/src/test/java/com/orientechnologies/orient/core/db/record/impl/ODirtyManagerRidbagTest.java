package com.orientechnologies.orient.core.db.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.junit.Test;

public class ODirtyManagerRidbagTest extends DBTestBase {

  @Test
  public void testRidBagTree() {
    Object value = YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValue();
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    try {
      YTDocument doc = new YTDocument();
      doc.field("test", "ddd");
      ORidBag bag = new ORidBag(db);
      YTDocument doc1 = new YTDocument();
      bag.add(doc1);
      doc.field("bag", bag);
      ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
      ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
      assertEquals(2, manager.getNewRecords().size());
    } finally {
      YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(value);
    }
  }
}
