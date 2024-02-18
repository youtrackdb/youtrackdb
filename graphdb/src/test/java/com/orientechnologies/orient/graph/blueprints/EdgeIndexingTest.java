package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public class EdgeIndexingTest {

  /** Test many to one connection type. Many vertices can be connected to only one. */
  @Test
  public void testOutLinksUniqueness() {
    final String url = "memory:.";
    var dbName = this.getClass().getSimpleName();
    try (var orientDB = new OrientDB(url, OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database ? memory users (admin identified by 'admin' role admin)", dbName);

      try (var session = orientDB.open(dbName, "admin", "admin")) {
        session.createEdgeClass("link");
        var vertexClass = session.createVertexClass("IndexedOutVertex");
        var property =
            vertexClass.createProperty(
                OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, "link"), OType.LINKBAG);
        property.createIndex(INDEX_TYPE.UNIQUE);

        session.begin();
        var vertexOutOne = session.newVertex("IndexedOutVertex");

        var vertexInOne = session.newVertex();
        var vertexInTwo = session.newVertex();

        vertexOutOne.addEdge(vertexInOne, "link");
        vertexOutOne.addEdge(vertexInTwo, "link");

        vertexOutOne.save();

        session.commit();

        session.begin();

        var vertexOutTwo = session.newVertex("IndexedOutVertex");
        vertexOutTwo.addEdge(vertexInTwo, "link");

        vertexOutTwo.save();
        try {
          session.commit();
          // in vertex can be linked by only one out vertex.
          Assert.fail();
        } catch (ORecordDuplicatedException e) {
        }
      }
    }
  }

  /** Test many to one connection type. Many vertices can be connected to only one. */
  @Test
  public void testOutLinksUniquenessTwo() {
    final String url = "memory:.";
    var dbName = this.getClass().getSimpleName();
    try (var orientDB = new OrientDB(url, OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database ? memory users (admin identified by 'admin' role admin)", dbName);
      try (var session = orientDB.open(dbName, "admin", "admin")) {
        session.createEdgeClass("link");
        OClass outVertexClass = session.createVertexClass("IndexedOutVertex");

        var property =
            outVertexClass.createProperty(
                OVertex.getDirectEdgeLinkFieldName(ODirection.OUT, "link"), OType.LINKBAG);
        property.createIndex(INDEX_TYPE.UNIQUE);

        session.begin();

        var vertexOutOne = session.newVertex("IndexedOutVertex");

        var vertexInOne = session.newVertex();
        var vertexInTwo = session.newVertex();

        vertexOutOne.addEdge(vertexInOne, "link");
        vertexOutOne.addEdge(vertexInTwo, "link");
        vertexOutOne.save();

        var vertexOutTwo = session.newVertex("IndexedOutVertex");
        vertexOutTwo.addEdge(vertexInTwo, "link");

        vertexOutTwo.save();
        try {
          session.commit();

          // in vertex can be linked by only one out vertex.
          Assert.fail();
        } catch (ORecordDuplicatedException e) {
        }
      }
    }
  }

  /** Test that "out_vertex" has edges to only single "in_vertex". */
  @Test
  public void testOutLinksUniquenessThree() {
    final String url = "memory:.";
    var dbName = this.getClass().getSimpleName();
    try (var orientDB = new OrientDB(url, OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database ? memory users (admin identified by 'admin' role admin)", dbName);
      try (var session = orientDB.open(dbName, "admin", "admin")) {
        session.createEdgeClass("link");

        OClass inVertexType = session.createVertexClass("IndexedInVertex");
        var in_link =
            inVertexType.createProperty(
                OVertex.getDirectEdgeLinkFieldName(ODirection.IN, "link"), OType.LINKBAG);

        inVertexType.createIndex("uniqueLinkIndex", "unique", in_link.getName());

        session.begin();

        var vertexOutOne = session.newVertex();

        var vertexInOne = session.newVertex("IndexedInVertex");
        var vertexInTwo = session.newVertex("IndexedInVertex");

        vertexOutOne.addEdge(vertexInOne, "link");
        vertexOutOne.addEdge(vertexInTwo, "link");

        vertexOutOne.save();

        try {
          session.commit();
          Assert.fail();
          // in vertex can be linked by only one out vertex.
        } catch (ORecordDuplicatedException e) {
        }
      }
    }
  }

  /** Test that "out_vertex" has singe and only single edge to "in_vertex". */
  @Test
  public void testOutLinksUniquenessFour() {
    final String url = "memory:.";
    var dbName = this.getClass().getSimpleName();
    try (var orientDB = new OrientDB(url, OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database ? memory users (admin identified by 'admin' role admin)", dbName);
      try (var session = orientDB.open(dbName, "admin", "admin")) {
        OClass edgeType = session.createEdgeClass("link");
        edgeType.createIndex("uniqueLinkIndex", "unique", OEdge.DIRECTION_IN, OEdge.DIRECTION_OUT);

        session.begin();
        var vertexOutOne = session.newVertex();

        var vertexInOne = session.newVertex();
        var vertexInTwo = session.newVertex();

        vertexOutOne.addEdge(vertexInOne, "link");
        vertexOutOne.addEdge(vertexInTwo, "link");
        vertexOutOne.addEdge(vertexInOne, "link");

        vertexOutOne.save();

        try {
          session.commit();
          Assert.fail();
          // in vertex can be linked by only one out vertex.
        } catch (ORecordDuplicatedException e) {
        }
      }
    }
  }
}
