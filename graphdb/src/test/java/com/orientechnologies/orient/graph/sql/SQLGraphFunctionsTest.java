/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SQLGraphFunctionsTest {
  private static OrientGraph graph;

  public SQLGraphFunctionsTest() {}

  @BeforeClass
  public static void beforeClass() {
    String url = "memory:" + SQLGraphFunctionsTest.class.getSimpleName();
    graph = new OrientGraph(url);

    OrientVertex v1 = graph.addVertex(null, "name", "A");
    OrientVertex v2 = graph.addVertex(null, "name", "B");
    OrientVertex v3 = graph.addVertex(null, "name", "C");
    OrientVertex v4 = graph.addVertex(null, "name", "D-D");
    OrientVertex v5 = graph.addVertex(null, "name", "E");
    OrientVertex v6 = graph.addVertex(null, "name", "F");

    v1.addEdge("label", v2, null, null, "weight", 10);
    v2.addEdge("label", v3, null, null, "weight", 20);
    v3.addEdge("label", v4, null, null, "weight", 30);
    v4.addEdge("label", v5, null, null, "weight", 40);
    v5.addEdge("label", v6, null, null, "weight", 50);
    v5.addEdge("label", v1, null, null, "weight", 100);

    graph.commit();
  }

  @AfterClass
  public static void afterClass() {
    graph.shutdown();
  }

  @Test
  public void checkDijkstra() {
    String subquery =
        "select $current, $target, Dijkstra($current, $target , 'weight') as path from V let"
            + " $target = ( select from V where name = \'C\' ) where 1 > 0";
    Iterable<OrientVertex> result =
        graph.command(new OSQLSynchQuery<OrientVertex>(subquery)).execute();
    Assert.assertTrue(result.iterator().hasNext());

    for (OrientVertex d : result) {

      OrientVertex $current = d.getProperty("$current");
      Object name = $current.getProperty("name");
      Iterable<OrientVertex> $target = (Iterable<OrientVertex>) d.getProperty("$target");
      Object name1 = $target.iterator().next().getProperty("name");
      System.out.println(
          "Shortest path from " + name + " and " + name1 + " is: " + d.getProperty("path"));
    }
  }

  @Test
  public void checkMinusInString() {
    OResultSet result = graph.sqlQuery("select expand( out()[name='D-D'] ) from V");
    Assert.assertTrue(result.hasNext());
  }

  @Test
  public void testTraversal() {
    try (OrientDB orientDB = new OrientDB("memory:", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database "
              + SQLGraphFunctionsTest.class.getSimpleName()
              + " memory users ( admin identified by 'admin' role admin)");
      try (var session =
          orientDB.open(SQLGraphFunctionsTest.class.getSimpleName(), "admin", "admin")) {
        session.createVertexClass("tc1");
        session.createEdgeClass("edge1");

        session.begin();

        OVertex v1 =
            session.command("create vertex tc1 SET id='1', name='name1'").next().getVertex().get();

        OVertex v2 =
            session.command("create vertex tc1 SET id='2', name='name2'").next().getVertex().get();

        session.commit();

        Iterator<OResult> e =
            session
                .command(
                    "create edge edge1 from "
                        + v1.getIdentity()
                        + " to "
                        + v2.getIdentity()
                        + " set f='fieldValue';")
                .stream()
                .iterator();
        session.commit();

        List<OElement> result =
            session.query("select outE() from tc1").stream().map(OResult::toElement).toList();

        Assert.assertEquals(2, result.size());

        var edgeId = e.next().toElement().getIdentity();
        Assert.assertEquals(1, result.get(0).<Collection<?>>getProperty("outE()").size());
        Assert.assertEquals(
            edgeId, result.get(0).<Collection<?>>getProperty("outE()").iterator().next());
        Assert.assertTrue(result.get(1).<Collection<?>>getProperty("outE()").isEmpty());
      }
    }
  }
}
