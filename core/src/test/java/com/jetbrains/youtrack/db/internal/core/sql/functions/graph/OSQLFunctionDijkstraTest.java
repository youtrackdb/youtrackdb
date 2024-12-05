package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.command.OBasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionDijkstraTest {

  private YouTrackDB youTrackDB;
  private YTDatabaseSession graph;

  private Vertex v1;
  private Vertex v2;
  private Vertex v3;
  private Vertex v4;
  private OSQLFunctionDijkstra functionDijkstra;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    functionDijkstra = new OSQLFunctionDijkstra();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    youTrackDB.close();
  }

  private void setUpDatabase() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            "OSQLFunctionDijkstraTest", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    graph =
        youTrackDB.open("OSQLFunctionDijkstraTest", "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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
    Edge e1 = graph.newEdge(v1, v2, "weight");
    e1.setProperty("weight", 1.0f);
    e1.save();

    Edge e2 = graph.newEdge(v2, v3, "weight");
    e2.setProperty("weight", 1.0f);
    e2.save();

    Edge e3 = graph.newEdge(v1, v3, "weight");
    e3.setProperty("weight", 100.0f);
    e3.save();

    Edge e4 = graph.newEdge(v3, v4, "weight");
    e4.setProperty("weight", 1.0f);
    e4.save();
    graph.commit();
  }

  @Test
  public void testExecute() throws Exception {
    v1 = graph.bindToSession(v1);
    v4 = graph.bindToSession(v4);

    var context = new OBasicCommandContext();
    context.setDatabase((YTDatabaseSessionInternal) graph);

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
