package com.orientechnologies.core.db.tool;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.record.YTVertex;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.storage.impl.local.OStorageRecoverEventListener;
import java.util.Objects;
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
    public void onScannedEdge(YTEntityImpl edge) {
      scannedEdges++;
    }

    @Override
    public void onRemovedEdge(YTEntityImpl edge) {
      removedEdges++;
    }

    @Override
    public void onScannedVertex(YTEntityImpl vertex) {
      scannedVertices++;
    }

    @Override
    public void onScannedLink(YTIdentifiable link) {
      scannedLinks++;
    }

    @Override
    public void onRemovedLink(YTIdentifiable link) {
      removedLinks++;
    }

    @Override
    public void onRepairedVertex(YTEntityImpl vertex) {
      repairedVertices++;
    }
  }

  private void init(YTDatabaseSession session) {
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
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverPerfectGraphNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverPerfectGraphNonLW", "admin", "admin")) {
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
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverBrokenGraphAllEdges"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverBrokenGraphAllEdges", "admin", "admin")) {
        init(session);

        session.begin();
        for (var e :
            session.query("select from E").stream()
                .map(YTResult::toEntity)
                .map(YTEntity::toEdge)
                .toList()) {
          e.<YTEntityImpl>getRecord().removeField("out");
          e.save();
        }
        session.commit();

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
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverBrokenGraphLinksInVerticesNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session =
          youTrackDB.open("testRecoverBrokenGraphLinksInVerticesNonLW", "admin", "admin")) {
        init(session);

        session.begin();
        for (var v :
            session.query("select from V").stream()
                .map(YTResult::toEntity)
                .filter(Objects::nonNull)
                .map(YTEntity::toVertex)
                .toList()) {
          for (String f : v.<YTEntityImpl>getRecord().fieldNames()) {
            if (f.startsWith(YTVertex.DIRECTION_OUT_PREFIX)) {
              v.<YTEntityImpl>getRecord().removeField(f);
              v.save();
            }
          }
        }
        session.commit();

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
