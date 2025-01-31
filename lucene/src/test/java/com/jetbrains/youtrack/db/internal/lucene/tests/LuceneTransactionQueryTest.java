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

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
public class LuceneTransactionQueryTest extends LuceneBaseTest {

  @Before
  public void init() {

    final var c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", PropertyType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");
    db.begin();
    db.save(doc);

    var query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }

    db.rollback();

    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    var query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    db.commit();

    db.begin();
    List<Result> results;
    try (var vertices = db.command(query)) {
      //noinspection resource
      results = vertices.stream().collect(Collectors.toList());
      assertThat(results).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");

    //noinspection OptionalGetWithoutIsPresent
    db.delete(results.get(0).getEntity().get().getIdentity());

    Collection coll;
    try (var vertices = db.query(query)) {
      try (var stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(coll).hasSize(0);
      assertThat(vertices).hasSize(0);
    }

    var iterator = coll.iterator();
    var i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);
    assertThat(index.getInternal().size(db)).isEqualTo(0);
    db.rollback();

    query = "select from C1 where search_fields(['p1'], 'abc' )=true ";

    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    assertThat(index.getInternal().size(db)).isEqualTo(1);
  }

  @Test
  public void txUpdateTest() {

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    var c1 = db.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(db);

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "update");

    db.save(doc);

    var query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    Assert.assertEquals(1, index.getInternal().size(db));

    db.commit();

    List<Result> results;
    try (var vertices = db.command(query)) {
      try (var resultStream = vertices.stream()) {
        results = resultStream.collect(Collectors.toList());
      }
    }

    Collection coll;
    try (var stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    assertThat(results).hasSize(1);
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size(db)).isEqualTo(1);

    db.begin();

    var record = results.get(0);
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    var element = db.bindToSession(record.getEntity().get());
    element.setProperty("p1", "removed");
    db.save(element);

    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
    Assert.assertEquals(1, index.getInternal().size(db));

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (var vertices = db.command(query)) {
      try (var stream = index.getInternal().getRids(db, "removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
    }

    Assert.assertEquals(1, coll.size());

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"update\")=true ";
    try (var vertices = db.command(query)) {
      try (var stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }
      assertThat(vertices).hasSize(1);
    }
    assertThat(coll).hasSize(1);
    assertThat(index.getInternal().size(db)).isEqualTo(1);
  }

  @Test
  public void txUpdateTestComplex() {

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    var c1 = db.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(db);

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");

    var doc1 = ((EntityImpl) db.newEntity("c1"));
    doc1.field("p1", "abc");

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    doc.field("p1", "removed");
    db.save(doc);

    var query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    Collection coll;
    try (var vertices = db.command(query)) {
      try (var stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(1, coll.size());
    }

    var iterator = coll.iterator();
    var i = 0;
    RecordId rid = null;
    while (iterator.hasNext()) {
      rid = (RecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(i, 1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getInternal().size(db), 2);

    query = "select from C1 where search_fields(['p1'], \"removed\")=true ";
    try (var vertices = db.command(query)) {
      try (var stream = index.getInternal().getRids(db, "removed")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 1);
    }

    db.rollback();

    query = "select from C1 where search_fields(['p1'], \"abc\")=true ";
    try (var vertices = db.command(query)) {
      assertThat(vertices).hasSize(2);
    }

    Assert.assertEquals(index.getInternal().size(db), 2);
  }
}
