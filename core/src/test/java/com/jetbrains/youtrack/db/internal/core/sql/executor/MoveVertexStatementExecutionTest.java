package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 */
public class MoveVertexStatementExecutionTest {

  @Rule
  public TestName name = new TestName();

  private DatabaseSession db;

  private YouTrackDB youTrackDB;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase("test", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    db = youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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

    ResultSet rs = db.query("select from " + vertexClassName1);
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

    ResultSet rs = db.query("select from " + vertexClassName1);
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
