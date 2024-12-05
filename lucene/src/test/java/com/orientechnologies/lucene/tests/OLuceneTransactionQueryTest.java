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

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneTransactionQueryTest extends OLuceneBaseTest {

  @Before
  public void init() {

    final YTClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", YTType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "abc");
    db.begin();
    db.save(doc);

    String query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }

    db.rollback();

    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "abc");

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    db.commit();

    db.begin();
    List<YTResult> results;
    try (YTResultSet vertices = db.command(query)) {
      //noinspection resource
      results = vertices.stream().collect(Collectors.toList());
      assertThat(results).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    doc = new EntityImpl("c1");
    doc.field("p1", "abc");

    //noinspection OptionalGetWithoutIsPresent
    db.delete(results.get(0).getEntity().get().getIdentity());

    Collection coll;
    try (YTResultSet vertices = db.query(query)) {
      try (Stream<YTRID> stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(coll).hasSize(0);
      assertThat(vertices).hasSize(0);
    }

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);
    assertThat(index.getInternal().size(db)).isEqualTo(0);
    db.rollback();

    query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);
  }

  @Test
  public void txUpdateTest() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    YTClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate(db);
    } catch (IOException e) {
      e.printStackTrace();
    }

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "update");

    db.save(doc);

    String query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    Assert.assertEquals(1, index.getInternal().size(db));

    db.commit();

    List<YTResult> results;
    try (YTResultSet vertices = db.command(query)) {
      try (Stream<YTResult> resultStream = vertices.stream()) {
        results = resultStream.collect(Collectors.toList());
      }
    }

    Collection coll;
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(results).hasSize(1);
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    db.begin();

    YTResult record = results.get(0);
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    Entity element = db.bindToSession(record.getEntity().get());
    element.setProperty("p1", "removed");
    db.save(element);

    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
    Assert.assertEquals(1, index.getInternal().size(db));

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (YTResultSet vertices = db.command(query)) {
      try (Stream<YTRID> stream = index.getInternal().getRids(db, "removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
    }

    Assert.assertEquals(1, coll.size());

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (YTResultSet vertices = db.command(query)) {
      try (Stream<YTRID> stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }
      assertThat(vertices).hasSize(1);
    }
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size(db)).isEqualTo(1);
  }

  @Test
  public void txUpdateTestComplex() {

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    YTClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate(db);
    } catch (IOException e) {
      e.printStackTrace();
    }

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "abc");

    EntityImpl doc1 = new EntityImpl("c1");
    doc1.field("p1", "abc");

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    doc.field("p1", "removed");
    db.save(doc);

    String query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    Collection coll;
    try (YTResultSet vertices = db.command(query)) {
      try (Stream<YTRID> stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(1, coll.size());
    }

    Iterator iterator = coll.iterator();
    int i = 0;
    YTRecordId rid = null;
    while (iterator.hasNext()) {
      rid = (YTRecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(i, 1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getInternal().size(db), 2);

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (YTResultSet vertices = db.command(query)) {
      try (Stream<YTRID> stream = index.getInternal().getRids(db, "removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 1);
    }

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    try (YTResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(2);
    }

    Assert.assertEquals(index.getInternal().size(db), 2);
  }
}
