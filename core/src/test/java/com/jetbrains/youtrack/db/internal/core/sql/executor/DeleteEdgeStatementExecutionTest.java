package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DeleteEdgeStatementExecutionTest extends DbTestBase {

  @Test
  public void testDeleteSingleEdge() {
    var vertexClassName = "testDeleteSingleEdgeV";
    session.createVertexClass(vertexClassName);

    var edgeClassName = "testDeleteSingleEdgeE";
    session.createEdgeClass(edgeClassName);

    Vertex prev = null;
    for (var i = 0; i < 10; i++) {
      session.begin();
      var v1 = session.newVertex(vertexClassName);
      v1.setProperty("name", "a" + i);
      if (prev != null) {
        prev = session.bindToSession(prev);
        prev.addEdge(v1, edgeClassName);
      }
      prev = v1;
      session.commit();
    }

    var rs = session.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    session.begin();
    session.command(
            "DELETE EDGE "
                + edgeClassName
                + " from (SELECT FROM "
                + vertexClassName
                + " where name = 'a1') to (SELECT FROM "
                + vertexClassName
                + " where name = 'a2')")
        .close();
    session.commit();

    rs = session.query("SELECT FROM " + edgeClassName);
    Assert.assertEquals(8, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(out()) FROM " + vertexClassName + " where name = 'a1'");
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(in()) FROM " + vertexClassName + " where name = 'a2'");
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }

  @Test
  public void testDeleteAll() {
    var vertexClassName = "testDeleteAllV";
    session.createVertexClass(vertexClassName);

    var edgeClassName = "testDeleteAllE";
    session.createEdgeClass(edgeClassName);

    Vertex prev = null;
    for (var i = 0; i < 10; i++) {
      session.begin();
      var v1 = session.newVertex(vertexClassName);
      v1.setProperty("name", "a" + i);
      if (prev != null) {
        prev = session.bindToSession(prev);
        prev.addEdge(v1, edgeClassName);
      }
      prev = v1;
      session.commit();
    }

    var rs = session.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();

    session.begin();
    session.command("DELETE EDGE " + edgeClassName).close();
    session.commit();

    rs = session.query("SELECT FROM " + edgeClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(out()) FROM " + vertexClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = session.query("SELECT expand(in()) FROM " + vertexClassName);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }
}
