package com.orientechnologies.orient.core.record.impl;


import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests that {@link YTDocument} is serializable.
 *
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest extends BaseMemoryInternalDatabase {

  public void beforeTest() throws Exception {
    super.beforeTest();

    db.begin();
    final YTDocument doc = new YTDocument();
    doc.setProperty("name", "Artem");

    final YTDocument linkedDoc = new YTDocument();

    doc.setProperty("country", linkedDoc, YTType.LINK);
    doc.setProperty("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();
  }

  @Test(expected = YTDatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().set(db);
    YTDocument doc = new YTDocument();
    ORidBag rids = new ORidBag(db);
    rids.add(new YTRecordId(2, 3));
    rids.add(new YTRecordId(2, 4));
    rids.add(new YTRecordId(2, 5));
    rids.add(new YTRecordId(2, 6));
    List<YTDocument> docs = new ArrayList<YTDocument>();
    YTDocument doc1 = new YTDocument();
    doc1.setProperty("rids", rids);
    docs.add(doc1);
    YTDocument doc2 = new YTDocument();
    doc2.setProperty("text", "text");
    docs.add(doc2);
    doc.setProperty("emb", docs, YTType.EMBEDDEDLIST);
    doc.setProperty("some", "test");

    byte[] res = db.getSerializer().toStream(db, doc);
    db.getSerializer().fromStream(db, res, new YTDocument(), new String[]{});
  }
}
