package com.orientechnologies.core.record.impl;


import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests that {@link YTEntityImpl} is serializable.
 *
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest extends BaseMemoryInternalDatabase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.begin();
    final YTEntityImpl doc = new YTEntityImpl();
    doc.setProperty("name", "Artem");

    final YTEntityImpl linkedDoc = new YTEntityImpl();

    doc.setProperty("country", linkedDoc, YTType.LINK);
    doc.setProperty("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();
  }

  @Test(expected = YTDatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().set(db);
    YTEntityImpl doc = new YTEntityImpl();
    RidBag rids = new RidBag(db);
    rids.add(new YTRecordId(2, 3));
    rids.add(new YTRecordId(2, 4));
    rids.add(new YTRecordId(2, 5));
    rids.add(new YTRecordId(2, 6));
    List<YTEntityImpl> docs = new ArrayList<YTEntityImpl>();
    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.setProperty("rids", rids);
    docs.add(doc1);
    YTEntityImpl doc2 = new YTEntityImpl();
    doc2.setProperty("text", "text");
    docs.add(doc2);
    doc.setProperty("emb", docs, YTType.EMBEDDEDLIST);
    doc.setProperty("some", "test");

    byte[] res = db.getSerializer().toStream(db, doc);
    db.getSerializer().fromStream(db, res, new YTEntityImpl(), new String[]{});
  }
}
