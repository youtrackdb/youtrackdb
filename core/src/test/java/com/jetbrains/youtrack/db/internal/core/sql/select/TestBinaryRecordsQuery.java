package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
    session.addBlobCluster("BlobCluster");
  }

  @Test
  public void testSelectBinary() {
    session.begin();
    session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    session.commit();

    var res = session.query("select from cluster:BlobCluster");

    assertEquals(1, res.stream().count());
  }

  @Test
  public void testSelectRidBinary() {
    session.begin();
    session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    session.commit();

    var res = session.query("select @rid from cluster:BlobCluster");
    assertEquals(1, res.stream().count());
  }

  @Test
  public void testDeleteBinary() {
    session.begin();
    var rec = session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    session.commit();

    session.begin();
    var res = session.command("delete from (select from cluster:BlobCluster)");
    session.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      session.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testSelectDeleteBinary() {
    session.begin();
    var rec = session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    session.commit();

    session.getMetadata().getSchema().createClass("RecordPointer");

    session.begin();
    var doc = (EntityImpl) session.newEntity("RecordPointer");
    doc.field("ref", session.bindToSession(rec));
    session.commit();

    session.begin();
    var res =
        session.command(
            "delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)");
    session.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      session.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testDeleteFromSelectBinary() {
    session.begin();
    var rec = session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    var rec1 = session.save(session.newBlob("blabla".getBytes()), "BlobCluster");
    session.commit();

    session.getMetadata().getSchema().createClass("RecordPointer");

    session.begin();
    var doc = (EntityImpl) session.newEntity("RecordPointer");
    doc.field("ref", session.bindToSession(rec));
    session.commit();

    session.begin();
    var doc1 = (EntityImpl) session.newEntity("RecordPointer");
    doc1.field("ref", session.bindToSession(rec1));
    session.commit();

    session.begin();
    var res = session.command("delete from (select expand(ref) from RecordPointer)");
    assertEquals(2, (long) res.next().getProperty("count"));
    session.commit();

    try {
      session.load(rec.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }

    try {
      session.load(rec1.getIdentity());
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }
}
