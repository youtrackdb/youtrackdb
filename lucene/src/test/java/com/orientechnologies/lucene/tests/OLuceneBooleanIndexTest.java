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

import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneBooleanIndexTest extends OLuceneBaseTest {

  @Before
  public void init() {

    YTClass personClass = db.createVertexClass("Person");
    personClass.createProperty(db, "isDeleted", YTType.BOOLEAN);

    db.command("create index Person.isDeleted on Person (isDeleted) FULLTEXT ENGINE LUCENE")
        .close();

    for (int i = 0; i < 1000; i++) {
      YTVertex person = db.newVertex("Person");
      person.setProperty("isDeleted", i % 2 == 0);
      db.begin();
      db.save(person);
      db.commit();
    }
  }

  @Test
  public void shouldQueryBooleanField() {

    YTResultSet docs = db.query("select from Person where search_class('false') = true");

    List<YTResult> results = docs.stream().collect(Collectors.toList());
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
