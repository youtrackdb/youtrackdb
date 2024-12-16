package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import static java.util.Arrays.asList;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SQLFunctionShortestPathTest {

  private YouTrackDB youTrackDB;
  private DatabaseSession graph;

  private Map<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();
  private SQLFunctionShortestPath function;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    function = new SQLFunctionShortestPath();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    youTrackDB.close();
  }

  private void setUpDatabase() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "SQLFunctionShortestPath", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    graph =
        youTrackDB.open("SQLFunctionShortestPath", "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    graph.createEdgeClass("Edge1");
    graph.createEdgeClass("Edge2");

    graph.begin();
    var v1 = graph.newVertex();
    v1.save();
    vertices.put(1, v1);
    var v2 = graph.newVertex();
    v2.save();
    vertices.put(2, v2);
    var v3 = graph.newVertex();
    vertices.put(3, v3);
    var v4 = graph.newVertex();
    v4.save();
    vertices.put(4, v4);

    vertices.get(1).setProperty("node_id", "A");
    vertices.get(2).setProperty("node_id", "B");
    vertices.get(3).setProperty("node_id", "C");
    vertices.get(4).setProperty("node_id", "D");

    graph.newRegularEdge(vertices.get(1), vertices.get(2), "Edge1").save();
    graph.newRegularEdge(vertices.get(2), vertices.get(3), "Edge1").save();
    graph.newRegularEdge(vertices.get(3), vertices.get(1), "Edge2").save();
    graph.newRegularEdge(vertices.get(3), vertices.get(4), "Edge1").save();

    for (int i = 5; i <= 20; i++) {
      var v = graph.newVertex();
      v.save();
      vertices.put(i, v);
      vertices.get(i).setProperty("node_id", "V" + i);
      graph.newRegularEdge(vertices.get(i - 1), vertices.get(i), "Edge1").save();
      if (i % 2 == 0) {
        graph.newRegularEdge(vertices.get(i - 2), vertices.get(i), "Edge1").save();
      }
    }
    graph.commit();
  }

  @Test
  public void testExecute() {
    bindVertices();

    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4)},
            new BasicCommandContext());

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testExecuteOut() {
    bindVertices();

    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), "out", null},
            new BasicCommandContext());

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1() {
    bindVertices();

    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), null, "Edge1"},
            new BasicCommandContext());

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1AndEdge2() {
    bindVertices();

    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(4), "BOTH", asList("Edge1", "Edge2")},
            new BasicCommandContext());

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testLong() {
    bindVertices();

    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20)},
            new BasicCommandContext());

    Assert.assertEquals(11, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    int next = 2;
    for (int i = 4; i <= 20; i += 2) {
      Assert.assertEquals(vertices.get(i).getIdentity(), result.get(next++));
    }
  }

  @Test
  public void testMaxDepth1() {
    bindVertices();

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            new BasicCommandContext());

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth2() {
    bindVertices();

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            new BasicCommandContext());

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth3() {
    bindVertices();

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            new BasicCommandContext());

    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testMaxDepth4() {
    bindVertices();

    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
    final List<RID> result =
        function.execute(
            null,
            null,
            null,
            new Object[]{vertices.get(1), vertices.get(20), null, null, additionalParams},
            new BasicCommandContext());

    Assert.assertEquals(0, result.size());
  }

  private void bindVertices() {
    var newVertices = new HashMap<Integer, Vertex>();
    for (var entry : vertices.entrySet()) {
      newVertices.put(entry.getKey(), graph.bindToSession(entry.getValue()));
    }

    vertices = newVertices;
  }
}
