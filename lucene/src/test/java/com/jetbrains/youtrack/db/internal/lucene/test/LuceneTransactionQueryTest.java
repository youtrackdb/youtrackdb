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

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionQueryTest extends BaseLuceneTest {

  @Before
  public void init() {

    final SchemaClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", PropertyType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "abc");
    db.begin();
    db.save(doc);

    ResultSet vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(vertices.stream().count(), 1);
    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");
    Assert.assertEquals(vertices.stream().count(), 0);
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    EntityImpl doc = new EntityImpl("c1");
    doc.field("p1", "abc");

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    ResultSet vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Result result = vertices.next();
    db.begin();

    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, index.getInternal().size(db));

    doc = new EntityImpl("c1");
    doc.field("p1", "abc");

    db.delete(result.getIdentity().get());

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Collection coll;
    try (Stream<RID> rids = index.getInternal().getRids(db, "abc")) {
      coll = rids.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.stream().count(), 0);
    Assert.assertEquals(coll.size(), 0);

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(0, i);
    Assert.assertEquals(0, index.getInternal().size(db));

    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
  }

  @Test
  public void txUpdateTest() {

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    SchemaClass c1 = db.getMetadata().getSchema().getClass("C1");
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

    ResultSet vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(vertices.stream().count(), 1);

    Assert.assertEquals(index.getInternal().size(db), 1);

    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Collection coll;
    try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    Result res = vertices.next();
    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(coll.size(), 1);
    Assert.assertEquals(index.getInternal().size(db), 1);

    db.begin();

    Entity record = db.bindToSession(res.getEntity().get());
    record.setProperty("p1", "removed");
    db.save(record);

    vertices = db.query("select from C1 where p1 lucene \"update\" ");
    try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.stream().count(), 0);
    Assert.assertEquals(coll.size(), 0);

    Iterator iterator = coll.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(i, 0);

    Assert.assertEquals(index.getInternal().size(db), 1);

    vertices = db.query("select from C1 where p1 lucene \"removed\"");
    try (Stream<RID> stream = index.getInternal().getRids(db, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.stream().count(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(vertices.stream().count(), 1);

    Assert.assertEquals(index.getInternal().size(db), 1);
  }

  @Test
  public void txUpdateTestComplex() {

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    SchemaClass c1 = db.getMetadata().getSchema().getClass("C1");
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

    ResultSet vertices = db.query("select from C1 where p1 lucene \"abc\"");
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
    Assert.assertNotNull(doc1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getInternal().size(db), 2);

    vertices = db.query("select from C1 where p1 lucene \"removed\" ");
    try (Stream<RID> stream = index.getInternal().getRids(db, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.stream().count(), 1);
    Assert.assertEquals(coll.size(), 1);

    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(vertices.stream().count(), 2);

    Assert.assertEquals(index.getInternal().size(db), 2);
  }
}
