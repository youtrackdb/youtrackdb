package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 */
public class OMoveVertexStatementExecutionTest {

  @Rule
  public TestName name = new TestName();

  private YTDatabaseSession db;

  private YouTrackDB youTrackDB;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    db = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    db.close();
    youTrackDB.close();
  }

  @Test
  public void testMoveVertex() {
    String vertexClassName1 = "testMoveVertexV1";
    String vertexClassName2 = "testMoveVertexV2";
    String edgeClassName = "testMoveVertexE";
    db.createVertexClass(vertexClassName1);
    db.createVertexClass(vertexClassName2);
    db.createEdgeClass(edgeClassName);

    db.begin();
    db.command("create vertex " + vertexClassName1 + " set name = 'a'");
    db.command("create vertex " + vertexClassName1 + " set name = 'b'");
    db.command(
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    db.command(
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to class:"
            + vertexClassName2);
    db.commit();

    OResultSet rs = db.query("select from " + vertexClassName1);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select from " + vertexClassName2);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select expand(out()) from " + vertexClassName2);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select expand(in()) from " + vertexClassName1);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testMoveVertexBatch() {
    String vertexClassName1 = "testMoveVertexBatchV1";
    String vertexClassName2 = "testMoveVertexBatchV2";
    String edgeClassName = "testMoveVertexBatchE";
    db.createVertexClass(vertexClassName1);
    db.createVertexClass(vertexClassName2);
    db.createEdgeClass(edgeClassName);

    db.begin();
    db.command("create vertex " + vertexClassName1 + " set name = 'a'");
    db.command("create vertex " + vertexClassName1 + " set name = 'b'");
    db.command(
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    db.command(
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to class:"
            + vertexClassName2
            + " BATCH 2");
    db.commit();

    OResultSet rs = db.query("select from " + vertexClassName1);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select from " + vertexClassName2);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select expand(out()) from " + vertexClassName2);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select expand(in()) from " + vertexClassName1);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }
}
