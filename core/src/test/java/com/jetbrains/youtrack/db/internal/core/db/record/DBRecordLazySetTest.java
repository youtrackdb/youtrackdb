package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class DBRecordLazySetTest extends DbTestBase {

  private EntityImpl doc1;
  private EntityImpl doc2;

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.begin();
    doc1 =
        db.save(
            ((EntityImpl) db.newEntity()).field("doc1", "doc1"));
    doc2 =
        db.save(
            ((EntityImpl) db.newEntity()).field("doc2", "doc2"));
    db.save(
        ((EntityImpl) db.newEntity()).field("doc3", "doc3"));
    db.commit();
  }

  @Test
  public void testDocumentNotEmbedded() {
    db.begin();
    var set = new LinkSet((EntityImpl) db.newEntity());
    var doc = (EntityImpl) db.newEntity();
    set.add(doc);
    assertFalse(doc.isEmbedded());
    db.rollback();
  }

  @Test()
  public void testSetAddRemove() {
    db.begin();
    var set = new LinkSet((EntityImpl) db.newEntity());
    var doc = (EntityImpl) db.newEntity();
    set.add(doc);
    set.remove(doc);
    assertTrue(set.isEmpty());
    db.rollback();
  }

  @Test
  public void testSetRemoveNotPersistent() {
    db.begin();
    var set = new LinkSet((EntityImpl) db.newEntity());
    doc1 = db.bindToSession(doc1);
    doc2 = db.bindToSession(doc2);

    set.add(doc1);
    set.add(doc2);
    set.add(new RecordId(5, 1000));
    assertEquals(3, set.size());
    set.remove(new RecordId(5, 1000));
    assertEquals(2, set.size());
    db.rollback();
  }

  @Test(expected = ValidationException.class)
  public void testSetWithNotExistentRecordWithValidation() {
    var test = db.getMetadata().getSchema().createClass("test");
    var test1 = db.getMetadata().getSchema().createClass("test1");
    test.createProperty(db, "fi", PropertyType.LINKSET).setLinkedClass(db, test1);

    db.begin();
    var doc = (EntityImpl) db.newEntity(test);
    var set = new LinkSet(doc);
    set.add(new RecordId(5, 1000));
    doc.field("fi", set);
    db.save(doc);
    db.commit();
  }
}
