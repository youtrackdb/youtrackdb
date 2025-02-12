package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SQLFunctionDijkstraTest {

  private YouTrackDB youTrackDB;
  private DatabaseSession graph;

  private Vertex v1;
  private Vertex v2;
  private Vertex v3;
  private Vertex v4;
  private SQLFunctionDijkstra functionDijkstra;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    functionDijkstra = new SQLFunctionDijkstra();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    youTrackDB.close();
  }

  private void setUpDatabase() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "SQLFunctionDijkstraTest", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    graph =
        youTrackDB.open("SQLFunctionDijkstraTest", "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    graph.createEdgeClass("weight");

    v1 = graph.newVertex();
    v2 = graph.newVertex();
    v3 = graph.newVertex();
    v4 = graph.newVertex();

    v1.setProperty("node_id", "A");
    v2.setProperty("node_id", "B");
    v3.setProperty("node_id", "C");
    v4.setProperty("node_id", "D");

    graph.begin();
    var e1 = graph.newRegularEdge(v1, v2, "weight");
    e1.setProperty("weight", 1.0f);
    e1.save();

    var e2 = graph.newRegularEdge(v2, v3, "weight");
    e2.setProperty("weight", 1.0f);
    e2.save();

    var e3 = graph.newRegularEdge(v1, v3, "weight");
    e3.setProperty("weight", 100.0f);
    e3.save();

    var e4 = graph.newRegularEdge(v3, v4, "weight");
    e4.setProperty("weight", 1.0f);
    e4.save();
    graph.commit();
  }

  @Test
  public void testExecute() throws Exception {
    v1 = graph.bindToSession(v1);
    v4 = graph.bindToSession(v4);

    var context = new BasicCommandContext();
    context.setDatabaseSession((DatabaseSessionInternal) graph);

    final List<Vertex> result =
        functionDijkstra.execute(
            null, null, null, new Object[]{v1, v4, "'weight'"}, context);

    assertEquals(4, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v2, result.get(1));
    assertEquals(v3, result.get(2));
    assertEquals(v4, result.get(3));
  }
}
