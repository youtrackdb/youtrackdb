package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.StorageRecoverEventListener;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;

public class GraphRecoveringTest {

  private class TestListener implements StorageRecoverEventListener {

    public long scannedEdges = 0;
    public long removedEdges = 0;
    public long scannedVertices = 0;
    public long scannedLinks = 0;
    public long removedLinks = 0;
    public long repairedVertices = 0;

    @Override
    public void onScannedEdge(EntityImpl edge) {
      scannedEdges++;
    }

    @Override
    public void onRemovedEdge(EntityImpl edge) {
      removedEdges++;
    }

    @Override
    public void onScannedVertex(EntityImpl vertex) {
      scannedVertices++;
    }

    @Override
    public void onScannedLink(Identifiable link) {
      scannedLinks++;
    }

    @Override
    public void onRemovedLink(Identifiable link) {
      removedLinks++;
    }

    @Override
    public void onRepairedVertex(EntityImpl vertex) {
      repairedVertices++;
    }
  }

  private void init(DatabaseSession session) {
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
    try (YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverPerfectGraphNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverPerfectGraphNonLW", "admin", "admin")) {
        init(session);

        final TestListener eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

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
    try (YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverBrokenGraphAllEdges"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverBrokenGraphAllEdges", "admin", "admin")) {
        init(session);

        session.begin();
        for (var e :
            session.query("select from E").stream()
                .map(Result::toEntity)
                .map(Entity::toEdge)
                .toList()) {
          e.<EntityImpl>getRecord().removeField("out");
          e.save();
        }
        session.commit();

        final TestListener eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

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
    try (YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
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
                .map(Result::toEntity)
                .filter(Objects::nonNull)
                .map(Entity::toVertex)
                .toList()) {
          for (String f : v.<EntityImpl>getRecord().fieldNames()) {
            if (f.startsWith(Vertex.DIRECTION_OUT_PREFIX)) {
              v.<EntityImpl>getRecord().removeField(f);
              v.save();
            }
          }
        }
        session.commit();

        final TestListener eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

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
