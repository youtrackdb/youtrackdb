package com.orientechnologies.orient.core.record.impl;


import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests that {@link ODocument} is serializable.
 *
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest extends BaseMemoryInternalDatabase {
  public void beforeTest() {
    super.beforeTest();

    db.begin();
    final ODocument doc = new ODocument();
    doc.setProperty("name", "Artem");

    final ODocument linkedDoc = new ODocument();

    doc.setProperty("country", linkedDoc, OType.LINK);
    doc.setProperty("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();
  }

  @Test(expected = ODatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().set(db);
    ODocument doc = new ODocument();
    ORidBag rids = new ORidBag(db);
    rids.add(new ORecordId(2, 3));
    rids.add(new ORecordId(2, 4));
    rids.add(new ORecordId(2, 5));
    rids.add(new ORecordId(2, 6));
    List<ODocument> docs = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    doc1.setProperty("rids", rids);
    docs.add(doc1);
    ODocument doc2 = new ODocument();
    doc2.setProperty("text", "text");
    docs.add(doc2);
    doc.setProperty("emb", docs, OType.EMBEDDEDLIST);
    doc.setProperty("some", "test");

    byte[] res = db.getSerializer().toStream(db, doc);
    db.getSerializer().fromStream(db, res, new ODocument(), new String[]{});
  }
}
