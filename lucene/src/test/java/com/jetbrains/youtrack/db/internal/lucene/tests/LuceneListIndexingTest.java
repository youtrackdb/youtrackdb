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

package com.jetbrains.youtrack.db.internal.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneListIndexingTest extends LuceneBaseTest {

  @Before
  public void init() {

    Schema schema = session.getMetadata().getSchema();

    var person = schema.createClass("Person");
    person.createProperty(session, "name", PropertyType.STRING);
    person.createProperty(session, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    //noinspection EmptyTryBlock
    try (var command =
        session.command(
            "create index Person.name_tags on Person (name,tags) FULLTEXT ENGINE LUCENE")) {
    }

    var city = schema.createClass("City");
    city.createProperty(session, "name", PropertyType.STRING);
    city.createProperty(session, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    //noinspection EmptyTryBlock
    try (var command =
        session.command("create index City.tags on City (tags) FULLTEXT ENGINE LUCENE")) {
    }
  }

  @Test
  public void testIndexingList() {
    var schema = session.getMetadata().getSchema();

    // Rome
    var doc = ((EntityImpl) session.newEntity("City"));
    doc.field("name", "Rome");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));

    session.begin();
    session.commit();

    var tagsIndex = schema.getClassInternal("City").getClassIndex(session, "City.tags");
    Collection<?> coll;
    try (var stream = tagsIndex.getInternal().getRids(session, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    doc = session.load((RID) coll.iterator().next());

    assertThat(doc.<String>field("name")).isEqualTo("Rome");

    // London
    doc = ((EntityImpl) session.newEntity("City"));
    doc.field("name", "London");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));

    session.begin();
    session.commit();

    session.begin();
    try (var stream = tagsIndex.getInternal().getRids(session, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    doc = session.bindToSession(doc);
    // modify london: it is rainy
    List<String> tags = doc.field("tags");
    tags.remove("Sunny");
    tags.add("Rainy");

    session.commit();

    try (var stream = tagsIndex.getInternal().getRids(session, "Rainy")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (var stream = tagsIndex.getInternal().getRids(session, "Beautiful")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (var stream = tagsIndex.getInternal().getRids(session, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (var query = session.query("select from City where search_class('Beautiful') =true ")) {

      assertThat(query).hasSize(2);
    }
  }

  @Test
  @Ignore
  public void testCompositeIndexList() {
    session.begin();
    var schema = session.getMetadata().getSchema();

    var doc = ((EntityImpl) session.newEntity("Person"));
    doc.field("name", "Enrico");
    doc.field("tags", Arrays.asList("Funny", "Tall", "Geek"));

    session.commit();

    session.begin();
    var idx = schema.getClassInternal("Person").getClassIndex(session, "Person.name_tags");
    Collection<?> coll;
    try (var stream = idx.getInternal().getRids(session, "Enrico")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(3);

    doc = ((EntityImpl) session.newEntity("Person"));
    doc.field("name", "Jared");
    doc.field("tags", Arrays.asList("Funny", "Tall"));

    session.commit();

    session.begin();
    try (var stream = idx.getInternal().getRids(session, "Jared")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(2);

    List<String> tags = doc.field("tags");

    tags.remove("Funny");
    tags.add("Geek");

    session.commit();

    try (var stream = idx.getInternal().getRids(session, "Funny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (var stream = idx.getInternal().getRids(session, "Geek")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (var query =
        session.query("select from Person where search_class('name:Enrico') =true ")) {
      assertThat(query).hasSize(1);
      try (var queryTwo =
          session.query("select from (select from Person search_class('name:Enrico')=true)")) {

        assertThat(queryTwo).hasSize(1);
        try (var queryThree =
            session.query("select from Person where search_class('Jared')=true")) {

          assertThat(queryThree).hasSize(1);
          try (var queryFour =
              session.query("select from Person where search_class('Funny') =true")) {

            assertThat(queryFour).hasSize(1);
            try (var queryFive =
                session.query("select from Person where search_class('Geek')=true")) {

              assertThat(queryFive).hasSize(2);
              try (var querySix =
                  session.query(
                      "select from Person where search_class('(name:Enrico AND tags:Geek)"
                          + " ')=true")) {
                assertThat(querySix).hasSize(1);
              }
            }
          }
        }
      }
    }
  }
}
