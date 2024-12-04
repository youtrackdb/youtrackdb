package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Test;

public class YTRecordLazySetTest extends DBTestBase {

  private YTDocument doc1;
  private YTDocument doc2;
  private YTDocument doc3;
  private YTRID rid1;
  private YTRID rid2;
  private YTRID rid3;

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.begin();
    doc1 =
        db.save(
            new YTDocument().field("doc1", "doc1"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid1 = doc1.getIdentity();
    doc2 =
        db.save(
            new YTDocument().field("doc2", "doc2"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid2 = doc2.getIdentity();
    doc3 =
        db.save(
            new YTDocument().field("doc3", "doc3"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid3 = doc3.getIdentity();
    db.commit();
  }

  @Test()
  public void testDocumentNotEmbedded() {
    OSet set = new OSet(new YTDocument());
    YTDocument doc = new YTDocument();
    set.add(doc);
    assertFalse(doc.isEmbedded());
  }

  @Test()
  public void testSetAddRemove() {
    OSet set = new OSet(new YTDocument());
    YTDocument doc = new YTDocument();
    set.add(doc);
    set.remove(doc);
    assertTrue(set.isEmpty());
  }

  @Test
  public void testSetRemoveNotPersistent() {
    OSet set = new OSet(new YTDocument());
    doc1 = db.bindToSession(doc1);
    doc2 = db.bindToSession(doc2);

    set.add(doc1);
    set.add(doc2);
    set.add(new YTRecordId(5, 1000));
    assertEquals(set.size(), 3);
    set.remove(new YTRecordId(5, 1000));
    assertEquals(set.size(), 2);
  }

  @Test(expected = OValidationException.class)
  public void testSetWithNotExistentRecordWithValidation() {
    YTClass test = db.getMetadata().getSchema().createClass("test");
    YTClass test1 = db.getMetadata().getSchema().createClass("test1");
    test.createProperty(db, "fi", YTType.LINKSET).setLinkedClass(db, test1);

    db.begin();
    YTDocument doc = new YTDocument(test);
    OSet set = new OSet(doc);
    set.add(new YTRecordId(5, 1000));
    doc.field("fi", set);
    db.save(doc);
    db.commit();
  }
}
