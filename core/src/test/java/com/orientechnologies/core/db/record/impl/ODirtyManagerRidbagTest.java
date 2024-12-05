package com.orientechnologies.core.db.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.ODirtyManager;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Test;

public class ODirtyManagerRidbagTest extends DBTestBase {

  @Test
  public void testRidBagTree() {
    Object value = YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValue();
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    try {
      YTEntityImpl doc = new YTEntityImpl();
      doc.field("test", "ddd");
      RidBag bag = new RidBag(db);
      YTEntityImpl doc1 = new YTEntityImpl();
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
