package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphFunctionsTest {
  private static OVertex v1;
  private static OVertex v2;
  private static OVertex v3;
  private static OEdge e1;
  private static OEdge e2;

  private static OrientDB orientDB;

  public GraphFunctionsTest() {}

  @BeforeClass
  public static void before() {
    orientDB = new OrientDB("memory:GraphFunctionsTest", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database GraphFunctionsTest memory users "
            + "( admin identified by 'admin' role admin)");

    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      session.createEdgeClass("SubEdge");
      session.createEdgeClass("contains");

      session.createVertexClass("SubVertex");

      v1 = session.newVertex("SubVertex").save();
      v2 = session.newVertex("SubVertex").save();
      v3 = session.newVertex().save();

      e1 = session.newEdge(v1, v2, "SubEdge").save();
      e2 = session.newEdge(v1, v3, "contains").save();
    }
  }

  @AfterClass
  public static void after() {
    orientDB.close();
  }

  @Test
  public void testOut() {
    long found;
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      // V1
      found = session.query("select expand( out() ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 2);

      found =
          session.query("select expand( out('SubEdge') ) from " + v1.getIdentity()).stream()
              .count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( out('dddd') ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V2
      found = session.query("select expand( out() ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);
      // V3
      found = session.query("select expand( out() ) from " + v3.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);
    }
  }

  @Test
  public void testIn() {
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      long found;

      // V1
      found = session.query("select expand( in() ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V2
      found = session.query("select expand( in() ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( in('SubEdge') ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( in('dddd') ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V3
      found = session.query("select expand( in() ) from " + v3.getIdentity()).stream().count();
      Assert.assertEquals(found, 1);
    }
  }

  @Test
  public void testOutE() {
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      long found;

      // V1
      found = session.query("select expand( outE() ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 2);

      found =
          session.query("select expand( outE('SubEdge') ) from " + v1.getIdentity()).stream()
              .count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( outE('dddd') ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V2
      found = session.query("select expand( outE() ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);
      // V3
      found = session.query("select expand( outE() ) from " + v3.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);
    }
  }

  @Test
  public void testInE() {
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      long found;

      // V1
      found = session.query("select expand( inE() ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V2
      found = session.query("select expand( inE() ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( inE('SubEdge') ) from " + v2.getIdentity()).stream()
              .count();
      Assert.assertEquals(found, 1);

      found =
          session.query("select expand( inE('dddd') ) from " + v2.getIdentity()).stream().count();
      Assert.assertEquals(found, 0);

      // V3
      found = session.query("select expand( inE() ) from " + v3.getIdentity()).stream().count();
      Assert.assertEquals(found, 1);
    }
  }

  @Test
  public void testOutV() {
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      // V1
      var vertices = session.query("select expand( outE().outV() ) from " + v1.getIdentity());
      Assert.assertEquals(vertices.next().getRecordId(), v1);

      vertices = session.query("select expand( outE().inV() ) from " + v1.getIdentity());
      Assert.assertEquals(vertices.next().getRecordId(), v2);

      // V2
      vertices = session.query("select expand( inE().inV() ) from " + v2.getIdentity());
      Assert.assertEquals(vertices.next().getRecordId(), v2);

      vertices = session.query("select expand( inE().outV() ) from " + v2.getIdentity());
      Assert.assertEquals(vertices.next().getRecordId(), v1);
    }
  }

  @Test
  public void testOutEPolymorphic() {
    try (var session = orientDB.open("GraphFunctionsTest", "admin", "admin")) {
      long found;
      // V1
      found = session.query("select expand( outE('E') ) from " + v1.getIdentity()).stream().count();
      Assert.assertEquals(found, 2);
    }
  }
}
