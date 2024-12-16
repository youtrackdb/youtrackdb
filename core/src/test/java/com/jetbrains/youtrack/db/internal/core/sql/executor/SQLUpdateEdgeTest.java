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
package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class SQLUpdateEdgeTest extends DbTestBase {

  @Test
  public void testUpdateEdge() {

    db.command("create class V1 extends V").close();

    db.command("create class E1 extends E").close();

    // VERTEXES
    db.begin();
    Entity v1 = db.command("create vertex").next().getEntity().get();
    assertEquals(v1.getSchemaType().get().getName(), "V");

    Entity v2 = db.command("create vertex V1").next().getEntity().get();
    assertEquals(v2.getSchemaType().get().getName(), "V1");

    Entity v3 =
        db.command("create vertex set vid = 'v3', brand = 'fiat'").next().getEntity().get();

    assertEquals(v3.getSchemaType().get().getName(), "V");
    assertEquals(v3.getProperty("brand"), "fiat");

    Entity v4 =
        db.command("create vertex V1 set vid = 'v4',  brand = 'fiat',name = 'wow'")
            .next()
            .getEntity()
            .get();
    db.commit();

    v4 = db.bindToSession(v4);
    assertEquals(v4.getSchemaType().get().getName(), "V1");
    assertEquals(v4.getProperty("brand"), "fiat");
    assertEquals(v4.getProperty("name"), "wow");

    db.begin();
    ResultSet edges =
        db.command("create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity());
    Edge edge = edges.next().getEdge().get();
    assertFalse(edges.hasNext());
    assertEquals(edge.getSchemaType().get().getName(), "E1");
    db.commit();

    db.begin();
    db.command(
            "update edge E1 set out = "
                + v3.getIdentity()
                + ", in = "
                + v4.getIdentity()
                + " where @rid = "
                + edge.getIdentity())
        .close();
    db.commit();

    ResultSet result = db.query("select expand(out('E1')) from " + v3.getIdentity());
    Result vertex4 = result.next();
    Assert.assertEquals(vertex4.getProperty("vid"), "v4");

    result = db.query("select expand(in('E1')) from " + v4.getIdentity());
    Result vertex3 = result.next();
    Assert.assertEquals(vertex3.getProperty("vid"), "v3");

    result = db.query("select expand(out('E1')) from " + v1.getIdentity());
    Assert.assertEquals(result.stream().count(), 0);

    result = db.query("select expand(in('E1')) from " + v2.getIdentity());
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testUpdateEdgeOfTypeE() {
    // issue #6378
    db.begin();
    Vertex v1 = db.command("create vertex").next().toVertex();
    Vertex v2 = db.command("create vertex").next().toVertex();
    Vertex v3 = db.command("create vertex").next().toVertex();
    db.commit();

    db.begin();
    ResultSet edges =
        db.command("create edge E from " + v1.getIdentity() + " to " + v2.getIdentity());
    Edge edge = edges.next().toEdge();

    db.command("UPDATE EDGE " + edge.getIdentity() + " SET in = " + v3.getIdentity());
    db.commit();

    ResultSet result = db.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertEquals(result.next().getIdentity().get(), v3.getIdentity());

    result = db.query("select expand(in()) from " + v3.getIdentity());
    Assert.assertEquals(result.next().getIdentity().get(), v1.getIdentity());

    result = db.command("select expand(in()) from " + v2.getIdentity());
    Assert.assertFalse(result.hasNext());
  }
}
