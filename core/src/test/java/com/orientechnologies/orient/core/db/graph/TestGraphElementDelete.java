package com.orientechnologies.orient.core.db.graph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestGraphElementDelete {

  private OxygenDB oxygenDB;
  private ODatabaseSession database;

  @Before
  public void before() {
    oxygenDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    database = oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    database.close();
    oxygenDB.close();
  }

  @Test
  public void testDeleteVertex() {
    database.begin();
    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(vertex));
    database.commit();

    try {
      database.load(edge.getIdentity());
      Assert.fail();
    } catch (ORecordNotFoundException e) {
      // ignore
    }
  }

  @Test
  public void testDeleteEdge() {

    database.begin();
    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
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
    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.commit();

    database.begin();
    OElement instance = database.load(edge.getIdentity());

    var th =
        new Thread(
            () -> {
              try (var database =
                  oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
                database.begin();
                OElement element = database.load(edge.getIdentity());
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
    } catch (OConcurrentModificationException e) {
    }

    assertNotNull(database.load(edge.getIdentity()));
    assertNotNull(database.load(vertex.getIdentity()));
    assertNotNull(database.load(vertex1.getIdentity()));
    assertTrue(
        ((OVertex) database.load(vertex.getIdentity()))
            .getEdges(ODirection.OUT, "E")
            .iterator()
            .hasNext());
    assertTrue(
        ((OVertex) database.load(vertex1.getIdentity()))
            .getEdges(ODirection.IN, "E")
            .iterator()
            .hasNext());
  }
}
