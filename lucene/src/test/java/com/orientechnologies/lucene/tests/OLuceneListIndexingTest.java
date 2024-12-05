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

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class OLuceneListIndexingTest extends OLuceneBaseTest {

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();

    YTClass person = schema.createClass("Person");
    person.createProperty(db, "name", YTType.STRING);
    person.createProperty(db, "tags", YTType.EMBEDDEDLIST, YTType.STRING);
    //noinspection EmptyTryBlock
    try (YTResultSet command =
        db.command("create index Person.name_tags on Person (name,tags) FULLTEXT ENGINE LUCENE")) {
    }

    YTClass city = schema.createClass("City");
    city.createProperty(db, "name", YTType.STRING);
    city.createProperty(db, "tags", YTType.EMBEDDEDLIST, YTType.STRING);
    //noinspection EmptyTryBlock
    try (YTResultSet command =
        db.command("create index City.tags on City (tags) FULLTEXT ENGINE LUCENE")) {
    }
  }

  @Test
  public void testIndexingList() {

    YTSchema schema = db.getMetadata().getSchema();

    // Rome
    YTEntityImpl doc = new YTEntityImpl("City");
    doc.field("name", "Rome");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));

    db.begin();
    db.save(doc);
    db.commit();

    OIndex tagsIndex = schema.getClass("City").getClassIndex(db, "City.tags");
    Collection<?> coll;
    try (Stream<YTRID> stream = tagsIndex.getInternal().getRids(db, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    doc = db.load((YTRID) coll.iterator().next());

    assertThat(doc.<String>field("name")).isEqualTo("Rome");

    // London
    doc = new YTEntityImpl("City");
    doc.field("name", "London");
    doc.field("tags", Arrays.asList("Beautiful", "Touristic", "Sunny"));

    db.begin();
    db.save(doc);
    db.commit();

    db.begin();
    try (Stream<YTRID> stream = tagsIndex.getInternal().getRids(db, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    doc = db.bindToSession(doc);
    // modify london: it is rainy
    List<String> tags = doc.field("tags");
    tags.remove("Sunny");
    tags.add("Rainy");


    db.save(doc);
    db.commit();

    try (Stream<YTRID> stream = tagsIndex.getInternal().getRids(db, "Rainy")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (Stream<YTRID> stream = tagsIndex.getInternal().getRids(db, "Beautiful")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (Stream<YTRID> stream = tagsIndex.getInternal().getRids(db, "Sunny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (YTResultSet query = db.query("select from City where search_class('Beautiful') =true ")) {

      assertThat(query).hasSize(2);
    }
  }

  @Test
  @Ignore
  public void testCompositeIndexList() {
    db.begin();
    YTSchema schema = db.getMetadata().getSchema();

    YTEntityImpl doc = new YTEntityImpl("Person");
    doc.field("name", "Enrico");
    doc.field("tags", Arrays.asList("Funny", "Tall", "Geek"));


    db.save(doc);
    db.commit();

    db.begin();
    OIndex idx = schema.getClass("Person").getClassIndex(db, "Person.name_tags");
    Collection<?> coll;
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Enrico")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(3);

    doc = new YTEntityImpl("Person");
    doc.field("name", "Jared");
    doc.field("tags", Arrays.asList("Funny", "Tall"));

    db.save(doc);
    db.commit();

    db.begin();
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Jared")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(coll).hasSize(2);

    List<String> tags = doc.field("tags");

    tags.remove("Funny");
    tags.add("Geek");

    db.save(doc);
    db.commit();

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Funny")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(1);

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Geek")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(2);

    try (YTResultSet query =
        db.query("select from Person where search_class('name:Enrico') =true ")) {
      assertThat(query).hasSize(1);
      try (YTResultSet queryTwo =
          db.query("select from (select from Person search_class('name:Enrico')=true)")) {

        assertThat(queryTwo).hasSize(1);
        try (YTResultSet queryThree =
            db.query("select from Person where search_class('Jared')=true")) {

          assertThat(queryThree).hasSize(1);
          try (YTResultSet queryFour =
              db.query("select from Person where search_class('Funny') =true")) {

            assertThat(queryFour).hasSize(1);
            try (YTResultSet queryFive =
                db.query("select from Person where search_class('Geek')=true")) {

              assertThat(queryFive).hasSize(2);
              try (YTResultSet querySix =
                  db.query(
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
