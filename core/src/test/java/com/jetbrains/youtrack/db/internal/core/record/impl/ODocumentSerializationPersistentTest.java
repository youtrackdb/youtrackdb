package com.jetbrains.youtrack.db.internal.core.record.impl;


import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests that {@link EntityImpl} is serializable.
 *
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest extends BaseMemoryInternalDatabase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.begin();
    final EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "Artem");

    final EntityImpl linkedDoc = new EntityImpl();

    doc.setProperty("country", linkedDoc, YTType.LINK);
    doc.setProperty("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();
  }

  @Test(expected = YTDatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().set(db);
    EntityImpl doc = new EntityImpl();
    RidBag rids = new RidBag(db);
    rids.add(new YTRecordId(2, 3));
    rids.add(new YTRecordId(2, 4));
    rids.add(new YTRecordId(2, 5));
    rids.add(new YTRecordId(2, 6));
    List<EntityImpl> docs = new ArrayList<EntityImpl>();
    EntityImpl doc1 = new EntityImpl();
    doc1.setProperty("rids", rids);
    docs.add(doc1);
    EntityImpl doc2 = new EntityImpl();
    doc2.setProperty("text", "text");
    docs.add(doc2);
    doc.setProperty("emb", docs, YTType.EMBEDDEDLIST);
    doc.setProperty("some", "test");

    byte[] res = db.getSerializer().toStream(db, doc);
    db.getSerializer().fromStream(db, res, new EntityImpl(), new String[]{});
  }
}
