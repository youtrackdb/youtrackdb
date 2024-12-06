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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneBooleanIndexTest extends LuceneBaseTest {

  @Before
  public void init() {

    SchemaClass personClass = db.createVertexClass("Person");
    personClass.createProperty(db, "isDeleted", PropertyType.BOOLEAN);

    db.command("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE")
        .close();

    for (int i = 0; i < 1000; i++) {
      Vertex person = db.newVertex("Person");
      person.setProperty("isDeleted", i % 2 == 0);
      db.begin();
      db.save(person);
      db.commit();
    }
  }

  @Test
  public void shouldQueryBooleanField() {

    ResultSet docs = db.query("select from Person where search_class('false') = true");

    List<Result> results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(500);

    assertThat(results.get(0).<Boolean>getProperty("isDeleted")).isFalse();
    docs.close();

    docs = db.query("select from Person where search_class('true') = true");

    results = docs.stream().collect(Collectors.toList());
    assertThat(results).hasSize(500);
    assertThat(results.get(0).<Boolean>getProperty("isDeleted")).isTrue();
    docs.close();
  }
}
