package com.orientechnologies.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTRecordBytes;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestBinaryRecordsQuery extends DBTestBase {

  @Before
  public void beforeTest() throws Exception {
    super.beforeTest();
    db.addBlobCluster("BlobCluster");
  }

  @Test
  public void testSelectBinary() {
    db.begin();
    db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    YTResultSet res = db.query("select from cluster:BlobCluster");

    assertEquals(1, res.stream().count());
  }

  @Test
  public void testSelectRidBinary() {
    db.begin();
    db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    YTResultSet res = db.query("select @rid from cluster:BlobCluster");
    assertEquals(1, res.stream().count());
  }

  @Test
  public void testDeleteBinary() {
    db.begin();
    YTRecord rec = db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.begin();
    YTResultSet res = db.command("delete from (select from cluster:BlobCluster)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testSelectDeleteBinary() {
    db.begin();
    YTRecord rec = db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    YTResultSet res =
        db.command("delete from cluster:BlobCluster where @rid in (select ref from RecordPointer)");
    db.commit();

    assertEquals(1, (long) res.next().getProperty("count"));
    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testDeleteFromSelectBinary() {
    db.begin();
    YTRecord rec = db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    YTRecord rec1 = db.save(new YTRecordBytes("blabla".getBytes()), "BlobCluster");
    db.commit();

    db.getMetadata().getSchema().createClass("RecordPointer");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("RecordPointer");
    doc.field("ref", db.bindToSession(rec));
    db.save(doc);
    db.commit();

    db.begin();
    YTEntityImpl doc1 = new YTEntityImpl("RecordPointer");
    doc1.field("ref", db.bindToSession(rec1));
    db.save(doc1);
    db.commit();

    db.begin();
    YTResultSet res = db.command("delete from (select expand(ref) from RecordPointer)");
    assertEquals(2, (long) res.next().getProperty("count"));
    db.commit();

    try {
      db.load(rec.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }

    try {
      db.load(rec1.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }
  }
}
