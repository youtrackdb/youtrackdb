package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OUpdateEdgeStatementExecutionTest extends DBTestBase {

  @Test
  public void testUpdateEdge() {

    db.command("create class V1 extends V");

    db.command("create class E1 extends E");

    // VERTEXES

    db.begin();
    YTEntity v1;
    try (YTResultSet res1 = db.command("create vertex")) {
      YTResult r = res1.next();
      Assert.assertEquals(r.getProperty("@class"), "V");
      v1 = r.toEntity();
    }
    db.commit();

    db.begin();
    YTEntity v2;
    try (YTResultSet res2 = db.command("create vertex V1")) {
      YTResult r = res2.next();
      Assert.assertEquals(r.getProperty("@class"), "V1");
      v2 = r.toEntity();
    }
    db.commit();

    db.begin();
    YTEntity v3;
    try (YTResultSet res3 = db.command("create vertex set vid = 'v3', brand = 'fiat'")) {
      YTResult r = res3.next();
      Assert.assertEquals(r.getProperty("@class"), "V");
      Assert.assertEquals(r.getProperty("brand"), "fiat");
      v3 = r.toEntity();
    }
    db.commit();

    db.begin();
    YTEntity v4;
    try (YTResultSet res4 =
        db.command("create vertex V1 set vid = 'v4',  brand = 'fiat',name = 'wow'")) {
      YTResult r = res4.next();
      Assert.assertEquals(r.getProperty("@class"), "V1");
      Assert.assertEquals(r.getProperty("brand"), "fiat");
      Assert.assertEquals(r.getProperty("name"), "wow");
      v4 = r.toEntity();
    }
    db.commit();

    db.begin();
    YTResultSet edges =
        db.command("create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity());
    db.commit();

    Assert.assertTrue(edges.hasNext());
    YTResult edge = edges.next();
    Assert.assertFalse(edges.hasNext());
    Assert.assertEquals(((YTDocument) edge.toEntity().getRecord()).getClassName(), "E1");
    edges.close();

    db.begin();
    db.command(
        "update edge E1 set out = "
            + v3.getIdentity()
            + ", in = "
            + v4.getIdentity()
            + " where @rid = "
            + edge.toEntity().getIdentity());
    db.commit();

    YTResultSet result = db.query("select expand(out('E1')) from " + v3.getIdentity());
    Assert.assertTrue(result.hasNext());
    YTResult vertex4 = result.next();
    Assert.assertEquals(vertex4.getProperty("vid"), "v4");
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(in('E1')) from " + v4.getIdentity());
    Assert.assertTrue(result.hasNext());
    YTResult vertex3 = result.next();
    Assert.assertEquals(vertex3.getProperty("vid"), "v3");
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(out('E1')) from " + v1.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select expand(in('E1')) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpdateEdgeOfTypeE() {
    // issue #6378

    db.begin();
    YTVertex v1 = db.newVertex();
    db.save(v1);
    YTVertex v2 = db.newVertex();
    db.save(v2);
    YTVertex v3 = db.newVertex();
    db.save(v3);
    db.commit();

    db.begin();
    YTResultSet edges =
        db.command("create edge E from " + v1.getIdentity() + " to " + v2.getIdentity());
    db.commit();
    YTResult edge = edges.next();

    db.begin();
    db.command("UPDATE EDGE " + edge.toEntity().getIdentity() + " SET in = " + v3.getIdentity())
        .close();
    db.commit();
    edges.close();

    YTResultSet result = db.query("select expand(out()) from " + v1.getIdentity());

    Assert.assertEquals(result.next().getRecordId(), v3.getIdentity());
    result.close();

    result = db.query("select expand(in()) from " + v3.getIdentity());
    Assert.assertEquals(result.next().getRecordId(), v1.getIdentity());
    result.close();

    result = db.command("select expand(in()) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
