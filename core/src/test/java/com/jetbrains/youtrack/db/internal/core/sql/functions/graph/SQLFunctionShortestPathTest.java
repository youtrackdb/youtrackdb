package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import static java.util.Arrays.asList;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SQLFunctionShortestPathTest {

  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal db;

  private Map<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();
  private SQLFunctionShortestPath function;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    function = new SQLFunctionShortestPath();
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    youTrackDB.close();
  }

  private void setUpDatabase() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "SQLFunctionShortestPath", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    db =
        (DatabaseSessionInternal) youTrackDB.open("SQLFunctionShortestPath", "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    db.createEdgeClass("Edge1");
    db.createEdgeClass("Edge2");

    db.begin();
    var v1 = db.newVertex();
    vertices.put(1, v1);
    var v2 = db.newVertex();
    vertices.put(2, v2);
    var v3 = db.newVertex();
    vertices.put(3, v3);
    var v4 = db.newVertex();
    vertices.put(4, v4);

    vertices.get(1).setProperty("node_id", "A");
    vertices.get(2).setProperty("node_id", "B");
    vertices.get(3).setProperty("node_id", "C");
    vertices.get(4).setProperty("node_id", "D");

    db.newStatefulEdge(vertices.get(1), vertices.get(2), "Edge1");
    db.newStatefulEdge(vertices.get(2), vertices.get(3), "Edge1");
    db.newStatefulEdge(vertices.get(3), vertices.get(1), "Edge2");
    db.newStatefulEdge(vertices.get(3), vertices.get(4), "Edge1");

    for (var i = 5; i <= 20; i++) {
      var v = db.newVertex();
      vertices.put(i, v);
      vertices.get(i).setProperty("node_id", "V" + i);
      db.newStatefulEdge(vertices.get(i - 1), vertices.get(i), "Edge1");
      if (i % 2 == 0) {
        db.newStatefulEdge(vertices.get(i - 2), vertices.get(i), "Edge1");
      }
    }
    db.commit();
  }

  @Test
  public void testExecute() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4)}, context
        );

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testExecuteOut() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), "out", null},
            context);

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), null, "Edge1"},
            context);

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1AndEdge2() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), "BOTH", asList("Edge1", "Edge2")},
            context);

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testLong() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20)},
            context);

    Assert.assertEquals(11, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    var next = 2;
    for (var i = 4; i <= 20; i += 2) {
      Assert.assertEquals(vertices.get(i).getIdentity(), result.get(next++));
    }
  }

  @Test
  public void testMaxDepth1() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            context);

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth2() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            context);

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth3() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            context);

    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testMaxDepth4() {
    bindVertices();

    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
    final var result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            context);

    Assert.assertEquals(0, result.size());
  }

  private void bindVertices() {
    var newVertices = new HashMap<Integer, Vertex>();
    for (var entry : vertices.entrySet()) {
      newVertices.put(entry.getKey(), db.bindToSession(entry.getValue()));
    }

    vertices = newVertices;
  }
}
