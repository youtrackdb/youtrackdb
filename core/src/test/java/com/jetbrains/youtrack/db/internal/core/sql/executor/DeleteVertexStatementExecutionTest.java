package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DeleteVertexStatementExecutionTest extends DbTestBase {

  @Test
  public void testDeleteSingleVertex() {
    var className = "testDeleteSingleVertex";
    db.createVertexClass(className);
    for (var i = 0; i < 10; i++) {
      db.begin();
      var v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className + " WHERE name = 'a3'").close();
    var rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();
    db.commit();
  }

  @Test
  public void testDeleteAllVertices() {
    var className = "testDeleteAllVertices";
    db.createVertexClass(className);
    for (var i = 0; i < 10; i++) {
      db.begin();
      var v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className).close();
    var rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
    db.commit();
  }

  @Test
  public void testFilterClass() {
    var className1 = "testDeleteAllVertices1";
    db.createVertexClass(className1);
    for (var i = 0; i < 10; i++) {
      db.begin();
      var v1 = db.newVertex(className1);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    var className2 = "testDeleteAllVertices2";
    db.createVertexClass(className2);
    for (var i = 0; i < 10; i++) {
      db.begin();
      var v1 = db.newVertex(className2);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className1).close();
    var rs = db.query("SELECT FROM " + className1);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
    db.commit();

    rs = db.query("SELECT FROM " + className2);
    Assert.assertEquals(10, rs.stream().count());
    rs.close();
  }
}
