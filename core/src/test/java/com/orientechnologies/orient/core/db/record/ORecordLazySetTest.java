package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

public class ORecordLazySetTest extends BaseMemoryDatabase {

  private ODocument doc1;
  private ODocument doc2;
  private ODocument doc3;
  private ORID rid1;
  private ORID rid2;
  private ORID rid3;

  public void beforeTest() {
    super.beforeTest();
    db.begin();
    doc1 =
        db.save(
            new ODocument().field("doc1", "doc1"), db.getClusterNameById(db.getDefaultClusterId()));
    rid1 = doc1.getIdentity();
    doc2 =
        db.save(
            new ODocument().field("doc2", "doc2"), db.getClusterNameById(db.getDefaultClusterId()));
    rid2 = doc2.getIdentity();
    doc3 =
        db.save(
            new ODocument().field("doc3", "doc3"), db.getClusterNameById(db.getDefaultClusterId()));
    rid3 = doc3.getIdentity();
    db.commit();
  }

  @Test()
  public void testDocumentNotEmbedded() {
    OSet set = new OSet(new ODocument());
    ODocument doc = new ODocument();
    set.add(doc);
    assertFalse(doc.isEmbedded());
  }

  @Test()
  public void testSetAddRemove() {
    OSet set = new OSet(new ODocument());
    ODocument doc = new ODocument();
    set.add(doc);
    set.remove(doc);
    assertTrue(set.isEmpty());
  }

  @Test
  public void testSetRemoveNotPersistent() {
    OSet set = new OSet(new ODocument());
    doc1 = db.bindToSession(doc1);
    doc2 = db.bindToSession(doc2);

    set.add(doc1);
    set.add(doc2);
    set.add(new ORecordId(5, 1000));
    assertEquals(set.size(), 3);
    set.remove(null);
    assertEquals(set.size(), 2);
  }

  @Test(expected = OValidationException.class)
  public void testSetWithNotExistentRecordWithValidation() {
    OClass test = db.getMetadata().getSchema().createClass("test");
    OClass test1 = db.getMetadata().getSchema().createClass("test1");
    test.createProperty("fi", OType.LINKSET).setLinkedClass(test1);

    db.begin();
    ODocument doc = new ODocument(test);
    OSet set = new OSet(doc);
    set.add(new ORecordId(5, 1000));
    doc.field("fi", set);
    db.save(doc);
    db.commit();
  }
}
