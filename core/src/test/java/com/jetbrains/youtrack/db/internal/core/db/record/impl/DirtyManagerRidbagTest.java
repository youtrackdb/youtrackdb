package com.jetbrains.youtrack.db.internal.core.db.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.DirtyManager;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import org.junit.Test;

public class DirtyManagerRidbagTest extends DbTestBase {

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
      EntityInternalUtils.convertAllMultiValuesToTrackedVersions(doc);
      DirtyManager manager = RecordInternal.getDirtyManager(doc1);
      assertEquals(2, manager.getNewRecords().size());
    } finally {
      GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(value);
    }
  }
}
