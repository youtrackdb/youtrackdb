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

    var type = session.createVertexClass("City");
    type.createProperty(session, "latitude", PropertyType.DOUBLE);
    type.createProperty(session, "longitude", PropertyType.DOUBLE);
    type.createProperty(session, "name", PropertyType.STRING);

    session.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void embeddedTx() {

    // THIS WON'T USE LUCENE INDEXES!!!! see #6997

    session.begin();
    var city = session.newVertex("City");
    city.setProperty("name", "London / a");

    city = session.newVertex("City");
    city.setProperty("name", "Rome");
    session.commit();

    session.begin();

    var resultSet = session.query("SELECT from City where name = 'London / a' ");

    Assertions.assertThat(resultSet).hasSize(1);

    resultSet = session.query("SELECT from City where name = 'Rome' ");

    Assertions.assertThat(resultSet).hasSize(1);
  }

  @Test
  public void testGetVericesFilterClass() {
    var v = session.getClass("V");
    v.createProperty(session, "name", PropertyType.STRING);
    session.command("CREATE INDEX V.name ON V(name) NOTUNIQUE");

    var oneClass = session.createVertexClass("One");
    var twoClass = session.createVertexClass("Two");

    var one = session.newVertex(oneClass);
    one.setProperty("name", "Same");

    session.begin();
    session.commit();

    var two = session.newVertex(twoClass);
    two.setProperty("name", "Same");
    session.begin();
    session.commit();

    var resultSet = session.query("SELECT from One where name = 'Same' ");

    Assertions.assertThat(resultSet).hasSize(1);
  }
}
