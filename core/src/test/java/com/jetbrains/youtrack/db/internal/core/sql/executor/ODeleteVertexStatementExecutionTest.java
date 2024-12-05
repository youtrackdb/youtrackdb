package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODeleteVertexStatementExecutionTest extends DBTestBase {

  @Test
  public void testDeleteSingleVertex() {
    String className = "testDeleteSingleVertex";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      db.begin();
      Vertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className + " WHERE name = 'a3'").close();
    YTResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();
    db.commit();
  }

  @Test
  public void testDeleteAllVertices() {
    String className = "testDeleteAllVertices";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      db.begin();
      Vertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className).close();
    YTResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
    db.commit();
  }

  @Test
  public void testFilterClass() {
    String className1 = "testDeleteAllVertices1";
    db.createVertexClass(className1);
    for (int i = 0; i < 10; i++) {
      db.begin();
      Vertex v1 = db.newVertex(className1);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    String className2 = "testDeleteAllVertices2";
    db.createVertexClass(className2);
    for (int i = 0; i < 10; i++) {
      db.begin();
      Vertex v1 = db.newVertex(className2);
      v1.setProperty("name", "a" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command("DELETE VERTEX " + className1).close();
    YTResultSet rs = db.query("SELECT FROM " + className1);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
    db.commit();

    rs = db.query("SELECT FROM " + className2);
    Assert.assertEquals(10, rs.stream().count());
    rs.close();
  }
}
