package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by tglman on 15/04/16.
 */
public class TestBinaryRecordsQuery extends BaseMemoryDatabase {

  @Before
  public void beforeTest() {
    super.beforeTest();
    db.addBlobCluster("BlobCluster");
  }

  @Test
  public void testSelectBinary() {
    db.begin();
    db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    OResultSet res = db.query("select from cluster:BlobCluster");

    assertEquals(1, res.stream().count());
  }

  @Test
  public void testSelectRidBinary() {
    db.begin();
    db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    OResultSet res = db.query("select @rid from cluster:BlobCluster");
    assertEquals(1, res.stream().count());
  }

  @Test
  public void testDeleteBinary() {
    db.begin();
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.begin();
    OResultSet res = db.command("delete from (select from cluster:BlobCluster)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    rec = db.load(rec.getIdentity());
    assertNull(rec);
  }

  @Test
  public void testSelectDeleteBinary() {
    db.begin();
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    ODocument doc = new ODocument("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    OResultSet res =
        db.command("delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    rec = db.load(rec.getIdentity());
    assertNull(rec);
  }

  @Test
  public void testDeleteFromSelectBinary() {
    db.begin();
    ORecord rec = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    ORecord rec1 = db.save(new ORecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    ODocument doc = new ODocument("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    ODocument doc1 = new ODocument("RecordPointer");
    doc1.field("ref", db.bindToSession(rec1));
    db.save(doc1);
    db.commit();

    db.begin();
    OResultSet res = db.command("delete from (select expand(ref) from RecordPointer)");
    assertEquals(2, (long) res.next().getProperty("count"));
    db.commit();

    rec = db.load(rec.getIdentity());
    assertNull(rec);
    rec = db.load(rec1.getIdentity());
    assertNull(rec);
  }
}
