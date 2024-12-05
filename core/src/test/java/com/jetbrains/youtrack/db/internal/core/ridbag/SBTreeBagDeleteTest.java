package com.jetbrains.youtrack.db.internal.core.ridbag;

import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.RID_BAG_SBTREEBONSAI_DELETE_DELAY;
import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SBTreeBagDeleteTest extends BaseMemoryInternalDatabase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.getConfiguration().setValue(RID_BAG_SBTREEBONSAI_DELETE_DELAY, 50);
  }

  @Test
  public void testDeleteRidbagTx() throws InterruptedException {

    EntityImpl doc = new EntityImpl();
    RidBag bag = new RidBag(db);
    int size =
        GlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    for (int i = 0; i < size; i++) {
      bag.add(new YTRecordId(10, i));
    }
    doc.field("bag", bag);

    db.begin();
    YTRID id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    doc = db.bindToSession(doc);
    bag = doc.field("bag");
    OBonsaiCollectionPointer pointer = bag.getPointer();

    db.begin();
    doc = db.bindToSession(doc);
    db.delete(doc);
    db.commit();

    try {
      db.load(id);
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }

    Thread.sleep(100);
    OSBTreeBonsai<YTIdentifiable, Integer> tree =
        db.getSbTreeCollectionManager().loadSBTree(pointer);
    assertEquals(0, tree.getRealBagSize(Collections.emptyMap()));
  }
}
