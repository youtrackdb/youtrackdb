package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.storage.impl.local.OStorageRecoverEventListener;
import org.junit.Assert;
import org.junit.Test;

public class GraphRecoveringTest {

  private class TestListener implements OStorageRecoverEventListener {

    public long scannedEdges = 0;
    public long removedEdges = 0;
    public long scannedVertices = 0;
    public long scannedLinks = 0;
    public long removedLinks = 0;
    public long repairedVertices = 0;

    @Override
    public void onScannedEdge(ODocument edge) {
      scannedEdges++;
    }

    @Override
    public void onRemovedEdge(ODocument edge) {
      removedEdges++;
    }

    @Override
    public void onScannedVertex(ODocument vertex) {
      scannedVertices++;
    }

    @Override
    public void onScannedLink(OIdentifiable link) {
      scannedLinks++;
    }

    @Override
    public void onRemovedLink(OIdentifiable link) {
      removedLinks++;
    }

    @Override
    public void onRepairedVertex(ODocument vertex) {
      repairedVertices++;
    }
  }

  private void init(ODatabaseSession session) {
    session.createVertexClass("V1");
    session.createVertexClass("V2");
    session.createEdgeClass("E1");
    session.createEdgeClass("E2");

    session.begin();
    var v0 = session.newVertex();
    v0.setProperty("key", 0);
    var v1 = session.newVertex("V1");
    v1.setProperty("key", 1);
    var v2 = session.newVertex("V2");
    v2.setProperty("key", 2);

    v0.addEdge(v1);
    v1.addEdge(v2, "E1");
    v2.addEdge(v0, "E2");

    v0.save();
    v1.save();
    v2.save();
    session.commit();
  }

  @Test
  public void testRecoverPerfectGraphNonLW() {
    try (OrientDB orientDB = new OrientDB("embedded:./", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database testRecoverPerfectGraphNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = orientDB.open("testRecoverPerfectGraphNonLW", "admin", "admin")) {
        init(session);

        final TestListener eventListener = new TestListener();

        new OGraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(eventListener.scannedEdges, 3);
        Assert.assertEquals(eventListener.removedEdges, 0);
        Assert.assertEquals(eventListener.scannedVertices, 3);
        Assert.assertEquals(eventListener.scannedLinks, 6);
        Assert.assertEquals(eventListener.removedLinks, 0);
        Assert.assertEquals(eventListener.repairedVertices, 0);
      }
    }
  }

  @Test
  public void testRecoverBrokenGraphAllEdges() {
    try (OrientDB orientDB = new OrientDB("embedded:./", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database testRecoverBrokenGraphAllEdges"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = orientDB.open("testRecoverBrokenGraphAllEdges", "admin", "admin")) {
        init(session);

        for (var e :
            session.query("select from E").stream()
                .map(OResult::toElement)
                .map(OElement::toEdge)
                .toList()) {
          session.begin();
          e.<ODocument>getRecord().removeField("out");
          e.save();
          session.commit();
        }

        final TestListener eventListener = new TestListener();

        new OGraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(eventListener.scannedEdges, 3);
        Assert.assertEquals(eventListener.removedEdges, 3);
        Assert.assertEquals(eventListener.scannedVertices, 3);
        // This is 3 because 3 referred by the edge are cleaned by the edge delete
        Assert.assertEquals(eventListener.scannedLinks, 3);
        // This is 3 because 3 referred by the edge are cleaned by the edge delete
        Assert.assertEquals(eventListener.removedLinks, 3);
        Assert.assertEquals(eventListener.repairedVertices, 3);
      }
    }
  }

  @Test
  public void testRecoverBrokenGraphLinksInVerticesNonLW() {
    try (OrientDB orientDB = new OrientDB("embedded:./", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database testRecoverBrokenGraphLinksInVerticesNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session =
          orientDB.open("testRecoverBrokenGraphLinksInVerticesNonLW", "admin", "admin")) {
        init(session);

        for (var v :
            session.query("select from V").stream()
                .map(OResult::toElement)
                .map(OElement::toVertex)
                .toList()) {
          for (String f : v.<ODocument>getRecord().fieldNames()) {
            if (f.startsWith(OVertex.DIRECTION_OUT_PREFIX)) {
              session.begin();
              v.<ODocument>getRecord().removeField(f);
              v.save();
              session.commit();
            }
          }
        }

        final TestListener eventListener = new TestListener();

        new OGraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(eventListener.scannedEdges, 3);
        Assert.assertEquals(eventListener.removedEdges, 3);
        Assert.assertEquals(eventListener.scannedVertices, 3);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(eventListener.scannedLinks, 0);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(eventListener.removedLinks, 0);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(eventListener.repairedVertices, 0);
      }
    }
  }
}
