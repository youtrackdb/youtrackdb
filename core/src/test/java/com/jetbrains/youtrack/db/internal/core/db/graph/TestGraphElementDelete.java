package com.jetbrains.youtrack.db.internal.core.db.graph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestGraphElementDelete {

  private YouTrackDB youTrackDB;
  private YTDatabaseSession database;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    database = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    database.close();
    youTrackDB.close();
  }

  @Test
  public void testDeleteVertex() {
    database.begin();
    Vertex vertex = database.newVertex("V");
    Vertex vertex1 = database.newVertex("V");
    Edge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(vertex));
    database.commit();

    try {
      database.load(edge.getIdentity());
      Assert.fail();
    } catch (YTRecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testDeleteEdge() {

    database.begin();
    Vertex vertex = database.newVertex("V");
    Vertex vertex1 = database.newVertex("V");
    Edge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(edge));
    database.commit();

    assertFalse(database.bindToSession(vertex).getEdges(ODirection.OUT, "E").iterator().hasNext());
  }

  @Test
  public void testDeleteEdgeConcurrentModification() throws Exception {
    database.begin();
    Vertex vertex = database.newVertex("V");
    Vertex vertex1 = database.newVertex("V");
    Edge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    Entity instance = database.load(edge.getIdentity());

    var th =
        new Thread(
            () -> {
              try (var database =
                  youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
                database.begin();
                Entity element = database.load(edge.getIdentity());
                element.setProperty("one", "two");
                database.save(element);
                database.commit();
              }
            });
    th.start();
    th.join();

    try {
      database.delete(instance);
      database.commit();
      Assert.fail();
    } catch (YTConcurrentModificationException e) {
    }

    assertNotNull(database.load(edge.getIdentity()));
    assertNotNull(database.load(vertex.getIdentity()));
    assertNotNull(database.load(vertex1.getIdentity()));
    assertTrue(
        ((Vertex) database.load(vertex.getIdentity()))
            .getEdges(ODirection.OUT, "E")
            .iterator()
            .hasNext());
    assertTrue(
        ((Vertex) database.load(vertex1.getIdentity()))
            .getEdges(ODirection.IN, "E")
            .iterator()
            .hasNext());
  }
}
