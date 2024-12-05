package com.orientechnologies.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.db.record.LinkSet;
import com.orientechnologies.core.exception.YTValidationException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Test;

public class YTRecordLazySetTest extends DBTestBase {

  private YTEntityImpl doc1;
  private YTEntityImpl doc2;
  private YTEntityImpl doc3;
  private YTRID rid1;
  private YTRID rid2;
  private YTRID rid3;

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.begin();
    doc1 =
        db.save(
            new YTEntityImpl().field("doc1", "doc1"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid1 = doc1.getIdentity();
    doc2 =
        db.save(
            new YTEntityImpl().field("doc2", "doc2"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid2 = doc2.getIdentity();
    doc3 =
        db.save(
            new YTEntityImpl().field("doc3", "doc3"),
            db.getClusterNameById(db.getDefaultClusterId()));
    rid3 = doc3.getIdentity();
    db.commit();
  }

  @Test()
  public void testDocumentNotEmbedded() {
    LinkSet set = new LinkSet(new YTEntityImpl());
    YTEntityImpl doc = new YTEntityImpl();
    set.add(doc);
    assertFalse(doc.isEmbedded());
  }

  @Test()
  public void testSetAddRemove() {
    LinkSet set = new LinkSet(new YTEntityImpl());
    YTEntityImpl doc = new YTEntityImpl();
    set.add(doc);
    set.remove(doc);
    assertTrue(set.isEmpty());
  }

  @Test
  public void testSetRemoveNotPersistent() {
    LinkSet set = new LinkSet(new YTEntityImpl());
    doc1 = db.bindToSession(doc1);
    doc2 = db.bindToSession(doc2);

    set.add(doc1);
    set.add(doc2);
    set.add(new YTRecordId(5, 1000));
    assertEquals(set.size(), 3);
    set.remove(new YTRecordId(5, 1000));
    assertEquals(set.size(), 2);
  }

  @Test(expected = YTValidationException.class)
  public void testSetWithNotExistentRecordWithValidation() {
    YTClass test = db.getMetadata().getSchema().createClass("test");
    YTClass test1 = db.getMetadata().getSchema().createClass("test1");
    test.createProperty(db, "fi", YTType.LINKSET).setLinkedClass(db, test1);

    db.begin();
    YTEntityImpl doc = new YTEntityImpl(test);
    LinkSet set = new LinkSet(doc);
    set.add(new YTRecordId(5, 1000));
    doc.field("fi", set);
    db.save(doc);
    db.commit();
  }
}
