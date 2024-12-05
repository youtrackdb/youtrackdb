/*
 *
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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class GraphDatabaseTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public GraphDatabaseTest(@Optional Boolean remote) {
    //super(remote != null && remote);
    super(true);
  }

  @Test
  public void populate() {
    generateGraphData();
  }

  @Test(dependsOnMethods = "populate")
  public void testSQLAgainstGraph() {
    database.createEdgeClass("drives");

    database.begin();
    YTVertex tom = database.newVertex();
    tom.setProperty("name", "Tom");
    tom.save();

    YTVertex ferrari = database.newVertex("GraphCar");
    ferrari.setProperty("brand", "Ferrari");
    ferrari.save();

    YTVertex maserati = database.newVertex("GraphCar");
    maserati.setProperty("brand", "Maserati");
    maserati.save();

    YTVertex porsche = database.newVertex("GraphCar");
    porsche.setProperty("brand", "Porsche");
    porsche.save();

    database.newEdge(tom, ferrari, "drives").save();
    database.newEdge(tom, maserati, "drives").save();
    database.newEdge(tom, porsche, "owns").save();

    database.commit();

    tom = database.bindToSession(tom);
    Assert.assertEquals(CollectionUtils.size(tom.getEdges(ODirection.OUT, "drives")), 2);

    YTResultSet result =
        database.query("select out_[in.@class = 'GraphCar'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result =
        database.query(
            "select out_[label='drives'][in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result = database.query("select out_[in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void testNotDuplicatedIndexTxChanges() throws IOException {
    YTClass oc = database.createVertexClass("vertexA");
    if (oc == null) {
      oc = database.createVertexClass("vertexA");
    }

    if (!oc.existsProperty("name")) {
      oc.createProperty(database, "name", YTType.STRING);
    }

    if (oc.getClassIndex(database, "vertexA_name_idx") == null) {
      oc.createIndex(database, "vertexA_name_idx", YTClass.INDEX_TYPE.UNIQUE, "name");
    }

    database.begin();
    YTVertex vertexA = database.newVertex("vertexA");
    vertexA.setProperty("name", "myKey");
    vertexA.save();

    YTVertex vertexB = database.newVertex("vertexA");
    vertexB.setProperty("name", "anotherKey");
    vertexB.save();
    database.commit();

    database.begin();
    database.bindToSession(vertexB).delete();
    database.bindToSession(vertexA).delete();

    var v = database.newVertex("vertexA");
    v.setProperty("name", "myKey");
    v.save();

    database.commit();
  }

  public void testNewVertexAndEdgesWithFieldsInOneShoot() {
    database.begin();
    YTVertex vertexA = database.newVertex();
    vertexA.setProperty("field1", "value1");
    vertexA.setProperty("field2", "value2");

    YTVertex vertexB = database.newVertex();
    vertexB.setProperty("field1", "value1");
    vertexB.setProperty("field2", "value2");

    YTEdge edgeC = database.newEdge(vertexA, vertexB);
    edgeC.setProperty("edgeF1", "edgeV2");

    database.commit();

    Assert.assertEquals(vertexA.getProperty("field1"), "value1");
    Assert.assertEquals(vertexA.getProperty("field2"), "value2");

    Assert.assertEquals(vertexB.getProperty("field1"), "value1");
    Assert.assertEquals(vertexB.getProperty("field2"), "value2");

    Assert.assertEquals(edgeC.getProperty("edgeF1"), "edgeV2");
  }

  @Test
  public void sqlNestedQueries() {
    database.begin();
    YTVertex vertex1 = database.newVertex();
    vertex1.setProperty("driver", "John");
    vertex1.save();

    YTVertex vertex2 = database.newVertex();
    vertex2.setProperty("car", "ford");
    vertex2.save();

    YTVertex targetVertex = database.newVertex();
    targetVertex.setProperty("car", "audi");
    targetVertex.save();

    YTEdge edge = database.newEdge(vertex1, vertex2);
    edge.setProperty("color", "red");
    edge.setProperty("action", "owns");
    edge.save();

    edge = database.newEdge(vertex1, targetVertex);
    edge.setProperty("color", "red");
    edge.setProperty("action", "wants");
    edge.save();

    database.commit();

    String query1 = "select driver from V where out().car contains 'ford'";
    YTResultSet result = database.query(query1);
    Assert.assertEquals(result.stream().count(), 1);

    String query2 = "select driver from V where outE()[color='red'].inV().car contains 'ford'";
    result = database.query(query2);
    Assert.assertEquals(result.stream().count(), 1);

    String query3 = "select driver from V where outE()[action='owns'].inV().car = 'ford'";
    result = database.query(query3);
    Assert.assertEquals(result.stream().count(), 1);

    String query4 =
        "select driver from V where outE()[color='red'][action='owns'].inV().car = 'ford'";
    result = database.query(query4);
    Assert.assertEquals(result.stream().count(), 1);
  }

  @SuppressWarnings("unchecked")
  public void nestedQuery() {
    database.createEdgeClass("owns");
    database.begin();

    YTVertex countryVertex1 = database.newVertex();
    countryVertex1.setProperty("name", "UK");
    countryVertex1.setProperty("area", "Europe");
    countryVertex1.setProperty("code", "2");
    countryVertex1.save();

    YTVertex cityVertex1 = database.newVertex();
    cityVertex1.setProperty("name", "leicester");
    cityVertex1.setProperty("lat", "52.64640");
    cityVertex1.setProperty("long", "-1.13159");
    cityVertex1.save();

    YTVertex cityVertex2 = database.newVertex();
    cityVertex2.setProperty("name", "manchester");
    cityVertex2.setProperty("lat", "53.47497");
    cityVertex2.setProperty("long", "-2.25769");

    database.newEdge(countryVertex1, cityVertex1, "owns").save();
    database.newEdge(countryVertex1, cityVertex2, "owns").save();

    database.commit();
    String subquery = "select out('owns') as out from V where name = 'UK'";
    List<YTResult> result = database.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((Collection) result.get(0).getProperty("out")).size(), 2);

    subquery = "select expand(out('owns')) from V where name = 'UK'";
    result = database.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (YTResult value : result) {
      Assert.assertTrue(value.hasProperty("lat"));
    }

    String query =
        "select name, lat, long, distance(lat,long,51.5,0.08) as distance from (select"
            + " expand(out('owns')) from V where name = 'UK') order by distance";
    result = database.query(query).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (YTResult oResult : result) {
      Assert.assertTrue(oResult.hasProperty("lat"));
      Assert.assertTrue(oResult.hasProperty("distance"));
    }
  }

  public void testDeleteOfVerticesWithDeleteCommandMustFail() {
    try {
      database.command("delete from GraphVehicle").close();
      Assert.fail();
    } catch (YTCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testInsertOfEdgeWithInsertCommand() {
    try {
      database.command(new OCommandSQL("insert into E set a = 33")).execute(database);
      Assert.fail();
    } catch (YTCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testInsertOfEdgeWithInsertCommandUnsafe() {
    database.begin();
    YTEntity insertedEdge =
        database
            .command("insert into E set in = #9:0, out = #9:1, a = 33 unsafe")
            .next()
            .toEntity();
    database.commit();

    Assert.assertNotNull(insertedEdge);

    database.begin();
    Long confirmDeleted =
        database
            .command("delete from " + insertedEdge.getIdentity() + " unsafe")
            .next()
            .<Long>getProperty("count");
    database.commit();

    Assert.assertEquals(confirmDeleted.intValue(), 1);
  }

  public void testEmbeddedDoc() {
    database.createClass("NonVertex");

    database.begin();
    YTVertex vertex = database.newVertex();
    vertex.setProperty("name", "vertexWithEmbedded");
    vertex.save();

    YTEntityImpl doc = new YTEntityImpl();
    doc.field("foo", "bar");
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    vertex.setProperty("emb1", doc);

    YTEntityImpl doc2 = new YTEntityImpl("V");
    doc2.field("foo", "bar1");
    vertex.setProperty("emb2", doc2, YTType.EMBEDDED);

    YTEntityImpl doc3 = new YTEntityImpl("NonVertex");
    doc3.field("foo", "bar2");
    vertex.setProperty("emb3", doc3, YTType.EMBEDDED);

    Object res1 = vertex.getProperty("emb1");
    Assert.assertNotNull(res1);
    Assert.assertTrue(res1 instanceof YTEntityImpl);

    Object res2 = vertex.getProperty("emb2");
    Assert.assertNotNull(res2);
    Assert.assertFalse(res2 instanceof YTEntityImpl);

    Object res3 = vertex.getProperty("emb3");
    Assert.assertNotNull(res3);
    Assert.assertTrue(res3 instanceof YTEntityImpl);
    database.commit();
  }
}
