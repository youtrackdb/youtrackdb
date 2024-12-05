package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODirtyManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import org.junit.Test;

public class ODirtyManagerRidbagTest extends DBTestBase {

  @Test
  public void testRidBagTree() {
    Object value = GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValue();
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    try {
      EntityImpl doc = new EntityImpl();
      doc.field("test", "ddd");
      RidBag bag = new RidBag(db);
      EntityImpl doc1 = new EntityImpl();
      bag.add(doc1);
      doc.field("bag", bag);
      ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
      ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
      assertEquals(2, manager.getNewRecords().size());
    } finally {
      GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(value);
    }
  }
}
