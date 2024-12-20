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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
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
public class GraphDatabaseTest extends BaseDBTest {

  @Parameters(value = "remote")
  public GraphDatabaseTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void populate() {
    generateGraphData();
  }

  @Test(dependsOnMethods = "populate")
  public void testSQLAgainstGraph() {
    db.createEdgeClass("drives");

    db.begin();
    Vertex tom = db.newVertex();
    tom.setProperty("name", "Tom");
    tom.save();

    Vertex ferrari = db.newVertex("GraphCar");
    ferrari.setProperty("brand", "Ferrari");
    ferrari.save();

    Vertex maserati = db.newVertex("GraphCar");
    maserati.setProperty("brand", "Maserati");
    maserati.save();

    Vertex porsche = db.newVertex("GraphCar");
    porsche.setProperty("brand", "Porsche");
    porsche.save();

    db.newRegularEdge(tom, ferrari, "drives").save();
    db.newRegularEdge(tom, maserati, "drives").save();
    db.newRegularEdge(tom, porsche, "owns").save();

    db.commit();

    tom = db.bindToSession(tom);
    Assert.assertEquals(CollectionUtils.size(tom.getEdges(Direction.OUT, "drives")), 2);

    ResultSet result =
        db.query("select out_[in.@class = 'GraphCar'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result =
        db.query(
            "select out_[label='drives'][in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result = db.query("select out_[in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void testNotDuplicatedIndexTxChanges() throws IOException {
    var oc = (SchemaClassInternal) db.createVertexClass("vertexA");
    if (oc == null) {
      oc = (SchemaClassInternal) db.createVertexClass("vertexA");
    }

    if (!oc.existsProperty("name")) {
      oc.createProperty(db, "name", PropertyType.STRING);
      oc.createIndex(db, "vertexA_name_idx", SchemaClass.INDEX_TYPE.UNIQUE, "name");
    }

    db.begin();
    Vertex vertexA = db.newVertex("vertexA");
    vertexA.setProperty("name", "myKey");
    vertexA.save();

    Vertex vertexB = db.newVertex("vertexA");
    vertexB.setProperty("name", "anotherKey");
    vertexB.save();
    db.commit();

    db.begin();
    db.bindToSession(vertexB).delete();
    db.bindToSession(vertexA).delete();

    var v = db.newVertex("vertexA");
    v.setProperty("name", "myKey");
    v.save();

    db.commit();
  }

  public void testNewVertexAndEdgesWithFieldsInOneShoot() {
    db.begin();
    Vertex vertexA = db.newVertex();
    vertexA.setProperty("field1", "value1");
    vertexA.setProperty("field2", "value2");

    Vertex vertexB = db.newVertex();
    vertexB.setProperty("field1", "value1");
    vertexB.setProperty("field2", "value2");

    Edge edgeC = db.newRegularEdge(vertexA, vertexB);
    edgeC.setProperty("edgeF1", "edgeV2");

    db.commit();

    Assert.assertEquals(vertexA.getProperty("field1"), "value1");
    Assert.assertEquals(vertexA.getProperty("field2"), "value2");

    Assert.assertEquals(vertexB.getProperty("field1"), "value1");
    Assert.assertEquals(vertexB.getProperty("field2"), "value2");

    Assert.assertEquals(edgeC.getProperty("edgeF1"), "edgeV2");
  }

  @Test
  public void sqlNestedQueries() {
    db.begin();
    Vertex vertex1 = db.newVertex();
    vertex1.setProperty("driver", "John");
    vertex1.save();

    Vertex vertex2 = db.newVertex();
    vertex2.setProperty("car", "ford");
    vertex2.save();

    Vertex targetVertex = db.newVertex();
    targetVertex.setProperty("car", "audi");
    targetVertex.save();

    Edge edge = db.newRegularEdge(vertex1, vertex2);
    edge.setProperty("color", "red");
    edge.setProperty("action", "owns");
    edge.save();

    edge = db.newRegularEdge(vertex1, targetVertex);
    edge.setProperty("color", "red");
    edge.setProperty("action", "wants");
    edge.save();

    db.commit();

    String query1 = "select driver from V where out().car contains 'ford'";
    ResultSet result = db.query(query1);
    Assert.assertEquals(result.stream().count(), 1);

    String query2 = "select driver from V where outE()[color='red'].inV().car contains 'ford'";
    result = db.query(query2);
    Assert.assertEquals(result.stream().count(), 1);

    String query3 = "select driver from V where outE()[action='owns'].inV().car = 'ford'";
    result = db.query(query3);
    Assert.assertEquals(result.stream().count(), 1);

    String query4 =
        "select driver from V where outE()[color='red'][action='owns'].inV().car = 'ford'";
    result = db.query(query4);
    Assert.assertEquals(result.stream().count(), 1);
  }

  @SuppressWarnings("unchecked")
  public void nestedQuery() {
    db.createEdgeClass("owns");
    db.begin();

    Vertex countryVertex1 = db.newVertex();
    countryVertex1.setProperty("name", "UK");
    countryVertex1.setProperty("area", "Europe");
    countryVertex1.setProperty("code", "2");
    countryVertex1.save();

    Vertex cityVertex1 = db.newVertex();
    cityVertex1.setProperty("name", "leicester");
    cityVertex1.setProperty("lat", "52.64640");
    cityVertex1.setProperty("long", "-1.13159");
    cityVertex1.save();

    Vertex cityVertex2 = db.newVertex();
    cityVertex2.setProperty("name", "manchester");
    cityVertex2.setProperty("lat", "53.47497");
    cityVertex2.setProperty("long", "-2.25769");

    db.newRegularEdge(countryVertex1, cityVertex1, "owns").save();
    db.newRegularEdge(countryVertex1, cityVertex2, "owns").save();

    db.commit();
    String subquery = "select out('owns') as out from V where name = 'UK'";
    List<Result> result = db.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((Collection) result.get(0).getProperty("out")).size(), 2);

    subquery = "select expand(out('owns')) from V where name = 'UK'";
    result = db.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Result value : result) {
      Assert.assertTrue(value.hasProperty("lat"));
    }

    String query =
        "select name, lat, long, distance(lat,long,51.5,0.08) as distance from (select"
            + " expand(out('owns')) from V where name = 'UK') order by distance";
    result = db.query(query).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (Result oResult : result) {
      Assert.assertTrue(oResult.hasProperty("lat"));
      Assert.assertTrue(oResult.hasProperty("distance"));
    }
  }

  public void testDeleteOfVerticesWithDeleteCommandMustFail() {
    try {
      db.command("delete from GraphVehicle").close();
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testInsertOfEdgeWithInsertCommand() {
    try {
      db.command(new CommandSQL("insert into E set a = 33")).execute(db);
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testInsertOfEdgeWithInsertCommandUnsafe() {
    db.begin();
    Entity insertedEdge =
        db
            .command("insert into E set in = #9:0, out = #9:1, a = 33 unsafe")
            .next()
            .toEntity();
    db.commit();

    Assert.assertNotNull(insertedEdge);

    db.begin();
    Long confirmDeleted =
        db
            .command("delete from " + insertedEdge.getIdentity() + " unsafe")
            .next()
            .<Long>getProperty("count");
    db.commit();

    Assert.assertEquals(confirmDeleted.intValue(), 1);
  }

  public void testEmbeddedDoc() {
    db.createClass("NonVertex");

    db.begin();
    Vertex vertex = db.newVertex();
    vertex.setProperty("name", "vertexWithEmbedded");
    vertex.save();

    EntityImpl doc = ((EntityImpl) db.newEntity());
    doc.field("foo", "bar");
    doc.save();

    vertex.setProperty("emb1", doc);

    EntityImpl doc2 = ((EntityImpl) db.newEntity("V"));
    doc2.field("foo", "bar1");
    vertex.setProperty("emb2", doc2, PropertyType.EMBEDDED);

    EntityImpl doc3 = ((EntityImpl) db.newEntity("NonVertex"));
    doc3.field("foo", "bar2");
    vertex.setProperty("emb3", doc3, PropertyType.EMBEDDED);

    Object res1 = vertex.getProperty("emb1");
    Assert.assertNotNull(res1);
    Assert.assertTrue(res1 instanceof EntityImpl);

    Object res2 = vertex.getProperty("emb2");
    Assert.assertNotNull(res2);
    Assert.assertFalse(res2 instanceof EntityImpl);

    Object res3 = vertex.getProperty("emb3");
    Assert.assertNotNull(res3);
    Assert.assertTrue(res3 instanceof EntityImpl);
    db.commit();
  }
}
