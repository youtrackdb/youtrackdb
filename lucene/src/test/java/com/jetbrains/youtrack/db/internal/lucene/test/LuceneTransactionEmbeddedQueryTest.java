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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.lucene.tests.LuceneBaseTest;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionEmbeddedQueryTest extends LuceneBaseTest {
  @Test
  public void testRollback() {
    createSchema(db);

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"abc"});
    db.begin();
    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    ResultSet vertices = db.query(query);

    Assert.assertEquals(1, vertices.stream().count());
    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.query(query);
    Assert.assertEquals(0, vertices.stream().count());
  }

  private static void createSchema(DatabaseSession db) {
    final SchemaClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void txRemoveTest() {
    createSchema(db);
    db.begin();

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"abc"});

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    String query = "select from C1 where p1 lucene \"abc\" ";
    ResultSet vertices = db.query(query);

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.query(query);

    Result res = vertices.next();
    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));

    db.delete(res.getIdentity().get());

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.query(query);

    Collection coll;
    try (Stream<RID> stream = index.getInternal().getRids(db, "abc")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(0, vertices.stream().count());
    Assert.assertEquals(0, coll.size());

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(0, i);
    Assert.assertEquals(0, index.getInternal().size(db));

    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.query(query);

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
  }

  @Test
  public void txUpdateTest() {
    createSchema(db);
    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.begin();
    Assert.assertEquals(0, index.getInternal().size(db));

    EntityImpl doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", new String[]{"update removed", "update fixed"});

    db.save(doc);

    String query = "select from C1 where p1 lucene \"update\" ";
    ResultSet vertices = db.query(query);

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(2, index.getInternal().size(db));

    db.commit();

    query = "select from C1 where p1 lucene \"update\" ";
    //noinspection deprecation
    vertices = db.query(query);

    Collection coll;
    try (final Stream<RID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    Result resultRecord = vertices.next();
    Assert.assertEquals(2, coll.size());
    Assert.assertEquals(2, index.getInternal().size(db));

    db.begin();

    // select in transaction while updating
    Entity record = db.bindToSession(resultRecord.getEntity().get());
    Collection p1 = record.getProperty("p1");
    p1.remove("update removed");
    db.save(record);

    query = "select from C1 where p1 lucene \"update\" ";
    vertices = db.query(query);
    try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(1, i);

    Assert.assertEquals(1, index.getInternal().size(db));

    query = "select from C1 where p1 lucene \"update\"";
    vertices = db.query(query);

    try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());

    Assert.assertEquals(1, vertices.stream().count());

    db.rollback();

    query = "select from C1 where p1 lucene \"update\" ";
    vertices = db.query(query);

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(2, index.getInternal().size(db));
  }

  @Test
  public void txUpdateTestComplex() {
    createSchema(db);
    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    Assert.assertEquals(0, index.getInternal().size(db));

    db.begin();

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
    ResultSet vertices = db.query(query);
    Collection coll;
    try (Stream<RID> stream = index.getInternal().getRids(db, "abc")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    Iterator iterator = coll.iterator();
    int i = 0;
    RecordId rid = null;
    while (iterator.hasNext()) {
      rid = (RecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(1, i);
    Assert.assertNotNull(doc1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(2, index.getInternal().size(db));

    query = "select from C1 where p1 lucene \"removed\" ";
    vertices = db.query(query);
    try (Stream<RID> stream = index.getInternal().getRids(db, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    db.rollback();

    query = "select from C1 where p1 lucene \"abc\" ";
    vertices = db.query(query);

    Assert.assertEquals(2, vertices.stream().count());

    Assert.assertEquals(2, index.getInternal().size(db));
  }
}
