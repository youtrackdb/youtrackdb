/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class GraphEmbeddedTest extends BaseLuceneTest {

  public GraphEmbeddedTest() {
  }

  @Before
  public void init() {

    SchemaClass type = db.createVertexClass("City");
    type.createProperty(db, "latitude", PropertyType.DOUBLE);
    type.createProperty(db, "longitude", PropertyType.DOUBLE);
    type.createProperty(db, "name", PropertyType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void embeddedTx() {

    // THIS WON'T USE LUCENE INDEXES!!!! see #6997

    db.begin();
    Vertex city = db.newVertex("City");
    city.setProperty("name", "London / a");
    db.save(city);

    city = db.newVertex("City");
    city.setProperty("name", "Rome");
    db.save(city);
    db.commit();

    db.begin();

    ResultSet resultSet = db.query("SELECT from City where name = 'London / a' ");

    Assertions.assertThat(resultSet).hasSize(1);

    resultSet = db.query("SELECT from City where name = 'Rome' ");

    Assertions.assertThat(resultSet).hasSize(1);
  }

  @Test
  public void testGetVericesFilterClass() {
    SchemaClass v = db.getClass("V");
    v.createProperty(db, "name", PropertyType.STRING);
    db.command("CREATE INDEX V.name ON V(name) NOTUNIQUE");

    SchemaClass oneClass = db.createVertexClass("One");
    SchemaClass twoClass = db.createVertexClass("Two");

    Vertex one = db.newVertex(oneClass);
    one.setProperty("name", "Same");

    db.begin();
    db.save(one);
    db.commit();

    Vertex two = db.newVertex(twoClass);
    two.setProperty("name", "Same");
    db.begin();
    db.save(two);
    db.commit();

    ResultSet resultSet = db.query("SELECT from One where name = 'Same' ");

    Assertions.assertThat(resultSet).hasSize(1);
  }
}
