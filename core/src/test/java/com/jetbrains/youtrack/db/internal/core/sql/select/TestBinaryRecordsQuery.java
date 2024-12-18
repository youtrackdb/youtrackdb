package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestBinaryRecordsQuery extends DbTestBase {

  @Before
  public void beforeTest() throws Exception {
    super.beforeTest();
    db.addBlobCluster("BlobCluster");
  }

  @Test
  public void testSelectBinary() {
    db.begin();
    db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    db.commit();

    ResultSet res = db.query("select from cluster:BlobCluster");

    assertEquals(1, res.stream().count());
  }

  @Test
  public void testSelectRidBinary() {
    db.begin();
    db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    db.commit();

    ResultSet res = db.query("select @rid from cluster:BlobCluster");
    assertEquals(1, res.stream().count());
  }

  @Test
  public void testDeleteBinary() {
    db.begin();
    Record rec = db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.begin();
    ResultSet res = db.command("delete from (select from cluster:BlobCluster)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testSelectDeleteBinary() {
    db.begin();
    Record rec = db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    EntityImpl doc = (EntityImpl) db.newEntity("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    ResultSet res =
        db.command("delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testDeleteFromSelectBinary() {
    db.begin();
    Record rec = db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    Record rec1 = db.save(db.newBlob("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    EntityImpl doc = (EntityImpl) db.newEntity("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    EntityImpl doc1 = (EntityImpl) db.newEntity("RecordPointer");
    doc1.field("ref", db.bindToSession(rec1));
    db.save(doc1);
    db.commit();

    db.begin();
    ResultSet res = db.command("delete from (select expand(ref) from RecordPointer)");
    assertEquals(2, (long) res.next().getProperty("count"));
    db.commit();

    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }

    try {
      db.load(rec1.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }
}
