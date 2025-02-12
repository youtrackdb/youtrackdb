/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 *
 */
public class SQLFunctionAstarTest {

  private static int dbCounter = 0;

  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal graph;

  private Vertex v0;
  private Vertex v1;
  private Vertex v2;
  private Vertex v3;
  private Vertex v4;
  private Vertex v5;
  private Vertex v6;
  private SQLFunctionAstar functionAstar;

  @Before
  public void setUp() throws Exception {

    setUpDatabase();

    functionAstar = new SQLFunctionAstar();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    youTrackDB.close();
  }

  private void setUpDatabase() {
    dbCounter++;

    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "SQLFunctionAstarTest", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    graph =
        (DatabaseSessionInternal)
            youTrackDB.open("SQLFunctionAstarTest", "admin",
                CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    graph.createEdgeClass("has_path");

    graph.begin();
    var cf = graph.getMetadata().getFunctionLibrary().createFunction("myCustomHeuristic");
    cf.setCode("return 1;");
    cf.save(graph);

    v0 = graph.newVertex();
    v1 = graph.newVertex();
    v2 = graph.newVertex();
    v3 = graph.newVertex();
    v4 = graph.newVertex();
    v5 = graph.newVertex();
    v6 = graph.newVertex();

    v0.setProperty("node_id", "Z"); // Tabriz
    v0.setProperty("name", "Tabriz");
    v0.setProperty("lat", 31.746512f);
    v0.setProperty("lon", 51.427002f);
    v0.setProperty("alt", 2200);

    v1.setProperty("node_id", "A"); // Tehran
    v1.setProperty("name", "Tehran");
    v1.setProperty("lat", 35.746512f);
    v1.setProperty("lon", 51.427002f);
    v1.setProperty("alt", 1800);

    v2.setProperty("node_id", "B"); // Mecca
    v2.setProperty("name", "Mecca");
    v2.setProperty("lat", 21.371244f);
    v2.setProperty("lon", 39.847412f);
    v2.setProperty("alt", 1500);

    v3.setProperty("node_id", "C"); // Bejin
    v3.setProperty("name", "Bejin");
    v3.setProperty("lat", 39.904041f);
    v3.setProperty("lon", 116.408011f);
    v3.setProperty("alt", 1200);

    v4.setProperty("node_id", "D"); // London
    v4.setProperty("name", "London");
    v4.setProperty("lat", 51.495065f);
    v4.setProperty("lon", -0.120850f);
    v4.setProperty("alt", 900);

    v5.setProperty("node_id", "E"); // NewYork
    v5.setProperty("name", "NewYork");
    v5.setProperty("lat", 42.779275f);
    v5.setProperty("lon", -74.641113f);
    v5.setProperty("alt", 1700);

    v6.setProperty("node_id", "F"); // Los Angles
    v6.setProperty("name", "Los Angles");
    v6.setProperty("lat", 34.052234f);
    v6.setProperty("lon", -118.243685f);
    v6.setProperty("alt", 400);

    var e1 = graph.newRegularEdge(v1, v2, "has_path");
    e1.setProperty("weight", 250.0f);
    e1.setProperty("ptype", "road");
    e1.save();
    var e2 = graph.newRegularEdge(v2, v3, "has_path");
    e2.setProperty("weight", 250.0f);
    e2.setProperty("ptype", "road");
    e2.save();
    var e3 = graph.newRegularEdge(v1, v3, "has_path");
    e3.setProperty("weight", 1000.0f);
    e3.setProperty("ptype", "road");
    e3.save();
    var e4 = graph.newRegularEdge(v3, v4, "has_path");
    e4.setProperty("weight", 250.0f);
    e4.setProperty("ptype", "road");
    e4.save();
    var e5 = graph.newRegularEdge(v2, v4, "has_path");
    e5.setProperty("weight", 600.0f);
    e5.setProperty("ptype", "road");
    e5.save();
    var e6 = graph.newRegularEdge(v4, v5, "has_path");
    e6.setProperty("weight", 400.0f);
    e6.setProperty("ptype", "road");
    e6.save();
    var e7 = graph.newRegularEdge(v5, v6, "has_path");
    e7.setProperty("weight", 300.0f);
    e7.setProperty("ptype", "road");
    e7.save();
    var e8 = graph.newRegularEdge(v3, v6, "has_path");
    e8.setProperty("weight", 200.0f);
    e8.setProperty("ptype", "road");
    e8.save();
    var e9 = graph.newRegularEdge(v4, v6, "has_path");
    e9.setProperty("weight", 900.0f);
    e9.setProperty("ptype", "road");
    e9.save();
    var e10 = graph.newRegularEdge(v2, v6, "has_path");
    e10.setProperty("weight", 2500.0f);
    e10.setProperty("ptype", "road");
    e10.save();
    var e11 = graph.newRegularEdge(v1, v5, "has_path");
    e11.setProperty("weight", 100.0f);
    e11.setProperty("ptype", "road");
    e11.save();
    var e12 = graph.newRegularEdge(v4, v1, "has_path");
    e12.setProperty("weight", 200.0f);
    e12.setProperty("ptype", "road");
    e12.save();
    var e13 = graph.newRegularEdge(v5, v3, "has_path");
    e13.setProperty("weight", 800.0f);
    e13.setProperty("ptype", "road");
    e13.save();
    var e14 = graph.newRegularEdge(v5, v2, "has_path");
    e14.setProperty("weight", 500.0f);
    e14.setProperty("ptype", "road");
    e14.save();
    var e15 = graph.newRegularEdge(v6, v5, "has_path");
    e15.setProperty("weight", 250.0f);
    e15.setProperty("ptype", "road");
    e15.save();
    var e16 = graph.newRegularEdge(v3, v1, "has_path");
    e16.setProperty("weight", 550.0f);
    e16.setProperty("ptype", "road");
    e16.save();
    graph.commit();
  }

  @Test
  public void test1Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    var ctx = new BasicCommandContext();

    v1 = graph.bindToSession(v1);
    v4 = graph.bindToSession(v4);

    ctx.setDatabaseSession(graph);
    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v1, v4, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(4, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v2, result.get(1));
    assertEquals(v3, result.get(2));
    assertEquals(v4, result.get(3));
  }

  @Test
  public void test2Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v1 = graph.bindToSession(v1);
    v6 = graph.bindToSession(v6);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v1, v6, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }
    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test3Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v1 = graph.bindToSession(v1);
    v6 = graph.bindToSession(v6);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v1, v6, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test4Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon", "alt"});
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v1 = graph.bindToSession(v1);
    v6 = graph.bindToSession(v6);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v1, v6, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test5Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v3 = graph.bindToSession(v3);
    v5 = graph.bindToSession(v5);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v3, v5, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(3, result.size());
    assertEquals(v3, result.get(0));
    assertEquals(v6, result.get(1));
    assertEquals(v5, result.get(2));
  }

  @Test
  public void test6Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test7Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, "EucliDEAN");
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test8Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.EUCLIDEANNOSQR);
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(5, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v4, result.get(3));
    assertEquals(v1, result.get(4));
  }

  @Test
  public void test9Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, Direction.BOTH);
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.MAXAXIS);
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);
    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(3, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v1, result.get(2));
  }

  @Test
  public void test10Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(SQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);
    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test11Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(SQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH, true);
    options.put(SQLFunctionAstar.PARAM_MAX_DEPTH, 3);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(SQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(0, result.size());
  }

  @Test
  public void test12Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(SQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(SQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(SQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(SQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH, false);
    options.put(SQLFunctionAstar.PARAM_MAX_DEPTH, 3);
    options.put(SQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[]{"has_path"});
    options.put(SQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[]{"lat", "lon"});
    options.put(SQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(SQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(graph);

    v6 = graph.bindToSession(v6);
    v1 = graph.bindToSession(v1);

    final List<Vertex> result =
        functionAstar.execute(null, null, null, new Object[]{v6, v1, "'weight'", options}, ctx);
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(4, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
  }

  @Test
  public void testSql() {
    var r =
        graph.query(
            "select expand(astar("
                + v1.getIdentity()
                + ", "
                + v4.getIdentity()
                + ", 'weight', {'direction':'out', 'parallel':true, 'edgeTypeNames':'has_path'}))");

    List<RID> result = new ArrayList<>();
    while (r.hasNext()) {
      result.add(r.next().getIdentity().get());
    }
    try (var rs = graph.query("select count(*) as count from has_path")) {
      assertEquals((Object) 16L, rs.next().getProperty("count"));
    }

    assertEquals(4, result.size());
    assertEquals(v1.getIdentity(), result.get(0));
    assertEquals(v2.getIdentity(), result.get(1));
    assertEquals(v3.getIdentity(), result.get(2));
    assertEquals(v4.getIdentity(), result.get(3));
  }
}
