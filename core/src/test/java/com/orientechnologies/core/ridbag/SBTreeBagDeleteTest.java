package com.orientechnologies.core.ridbag;

import static com.orientechnologies.core.config.YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_DELETE_DELAY;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
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

    YTEntityImpl doc = new YTEntityImpl();
    RidBag bag = new RidBag(db);
    int size =
        YTGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
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
