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
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionEmbeddedQueryTest extends LuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    final SchemaClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {
    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"abc"});
    db.begin();
    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true ";

    try (ResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }
    db.rollback();

    query = "select from C1 where search_class( \"abc\")=true  ";
    try (ResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(0);
    }
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"abc"});

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where search_class( \"abc\")=true";
    try (ResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(db), 1);
    }
    db.commit();

    db.begin();
    try (ResultSet vertices = db.command(query)) {

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(db), 1);
    }

    doc = db.bindToSession(doc);
    db.delete(doc);

    try (ResultSet vertices = db.command(query)) {

      Collection coll;
      try (Stream<RID> stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(0);
      Assert.assertEquals(coll.size(), 0);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(i, 0);
      Assert.assertEquals(index.getInternal().size(db), 0);
    }

    db.rollback();

    try (ResultSet vertices = db.command(query)) {

      assertThat(vertices).hasSize(1);

      Assert.assertEquals(index.getInternal().size(db), 1);
    }
  }

  @Test
  @Ignore
  public void txUpdateTest() {

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(index.getInternal().size(db), 0);

    db.begin();

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"update removed", "update fixed"});

    db.save(doc);

    String query = "select from C1 where search_class(\"update\")=true ";
    try (ResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
      Assert.assertEquals(index.getInternal().size(db), 2);
    }
    db.commit();

    Collection coll;
    try (ResultSet vertices = db.command(query)) {
      try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 2);
      Assert.assertEquals(index.getInternal().size(db), 2);
    }
    db.begin();

    // select in transaction while updating
    Collection p1 = doc.field("p1");
    p1.remove("update removed");
    db.save(doc);

    try (ResultSet vertices = db.command(query)) {
      try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }

      assertThat(vertices).hasSize(1);
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(index.getInternal().size(db), 1);
    }

    try (ResultSet vertices = db.command(query)) {
      try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }
      Assert.assertEquals(coll.size(), 1);
      assertThat(vertices).hasSize(1);
    }

    db.rollback();

    try (ResultSet vertices = db.command(query)) {
      assertThat(vertices).hasSize(1);
    }

    Assert.assertEquals(index.getInternal().size(db), 2);
  }

  @Test
  public void txUpdateTestComplex() {

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"abc"});

    EntityImpl doc1 = ((EntityImpl) db.newEntity("c1"));
    doc1.field("p1", new String[]{"abc"});

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    doc.field("p1", new String[]{"removed"});
    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\"";

    try (ResultSet vertices = db.query(query)) {
      Collection coll;
      try (Stream<RID> stream = index.getInternal().getRids(db, "abc")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.stream().count(), 1);
      Assert.assertEquals(coll.size(), 1);

      Iterator iterator = coll.iterator();
      int i = 0;
      RecordId rid = null;
      while (iterator.hasNext()) {
        rid = (RecordId) iterator.next();
        i++;
      }

      Assert.assertEquals(i, 1);
      Assert.assertNotNull(rid);
      Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
      Assert.assertEquals(index.getInternal().size(db), 2);
    }

    query = "select from C1 where p1 lucene \"removed\" ";
    try (ResultSet vertices = db.query(query)) {
      Collection coll;
      try (Stream<RID> stream = index.getInternal().getRids(db, "removed")) {
        coll = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(vertices.stream().count(), 1);
      Assert.assertEquals(coll.size(), 1);

      db.rollback();
    }

    query = "select from C1 where p1 lucene \"abc\" ";

    try (ResultSet vertices = db.query(query)) {

      Assert.assertEquals(vertices.stream().count(), 2);

      Assert.assertEquals(index.getInternal().size(db), 2);
    }
  }
}
