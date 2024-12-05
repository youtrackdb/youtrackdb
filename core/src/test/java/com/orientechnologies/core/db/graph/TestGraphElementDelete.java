package com.orientechnologies.core.db.graph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.exception.YTConcurrentModificationException;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.record.YTVertex;
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
    YTVertex vertex = database.newVertex("V");
    YTVertex vertex1 = database.newVertex("V");
    YTEdge edge = vertex.addEdge(vertex1, "E");
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
    YTVertex vertex = database.newVertex("V");
    YTVertex vertex1 = database.newVertex("V");
    YTEdge edge = vertex.addEdge(vertex1, "E");
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
    YTVertex vertex = database.newVertex("V");
    YTVertex vertex1 = database.newVertex("V");
    YTEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    YTEntity instance = database.load(edge.getIdentity());

    var th =
        new Thread(
            () -> {
              try (var database =
                  youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
                database.begin();
                YTEntity element = database.load(edge.getIdentity());
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
        ((YTVertex) database.load(vertex.getIdentity()))
            .getEdges(ODirection.OUT, "E")
            .iterator()
            .hasNext());
    assertTrue(
        ((YTVertex) database.load(vertex1.getIdentity()))
            .getEdges(ODirection.IN, "E")
            .iterator()
            .hasNext());
  }
}
