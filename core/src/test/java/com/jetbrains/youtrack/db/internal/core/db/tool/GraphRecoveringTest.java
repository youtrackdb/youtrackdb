package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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

    v0.addStateFulEdge(v1);
    v1.addEdge(v2, "E1");
    v2.addEdge(v0, "E2");

    v0.save();
    v1.save();
    v2.save();
    session.commit();
  }

  @Test
  public void testRecoverPerfectGraphNonLW() {
    try (YouTrackDB youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverPerfectGraphNonLW"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverPerfectGraphNonLW", "admin", "admin")) {
        init(session);

        final var eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(3, eventListener.scannedEdges);
        Assert.assertEquals(0, eventListener.removedEdges);
        Assert.assertEquals(3, eventListener.scannedVertices);
        Assert.assertEquals(6, eventListener.scannedLinks);
        Assert.assertEquals(0, eventListener.removedLinks);
        Assert.assertEquals(0, eventListener.repairedVertices);
      }
    }
  }

  @Test
  public void testRecoverBrokenGraphAllEdges() {
    try (YouTrackDB youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.execute(
          "create database testRecoverBrokenGraphAllEdges"
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session = youTrackDB.open("testRecoverBrokenGraphAllEdges", "admin", "admin")) {
        init(session);

        session.begin();
        for (var e :
            session.query("select from E").stream()
                .map(Result::castToStatefulEdge)
                .toList()) {
          e.<EntityImpl>getRecord(session).removeField("out");
        }
        session.commit();

        final var eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(3, eventListener.scannedEdges);
        Assert.assertEquals(3, eventListener.removedEdges);
        Assert.assertEquals(3, eventListener.scannedVertices);
        // This is 3 because 3 referred by the edge are cleaned by the edge delete
        Assert.assertEquals(3, eventListener.scannedLinks);
        // This is 3 because 3 referred by the edge are cleaned by the edge delete
        Assert.assertEquals(3, eventListener.removedLinks);
        Assert.assertEquals(3, eventListener.repairedVertices);
      }
    }
  }

  @Test
  public void testRecoverBrokenGraphLinksInVerticesNonLW() {
    try (YouTrackDB youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
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
                .map(Result::asEntity)
                .filter(Objects::nonNull)
                .map(Entity::castToVertex)
                .toList()) {
          for (var f : v.<EntityImpl>getRecord(session).fieldNames()) {
            if (f.startsWith(Vertex.DIRECTION_OUT_PREFIX)) {
              v.<EntityImpl>getRecord(session).removeField(f);
              v.save();
            }
          }
        }
        session.commit();

        final var eventListener = new TestListener();

        new GraphRepair().setEventListener(eventListener).repair(session, null, null);

        Assert.assertEquals(3, eventListener.scannedEdges);
        Assert.assertEquals(3, eventListener.removedEdges);
        Assert.assertEquals(3, eventListener.scannedVertices);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(0, eventListener.scannedLinks);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(0, eventListener.removedLinks);
        // This is 0 because the delete edge does the cleanup
        Assert.assertEquals(0, eventListener.repairedVertices);
      }
    }
  }
}
