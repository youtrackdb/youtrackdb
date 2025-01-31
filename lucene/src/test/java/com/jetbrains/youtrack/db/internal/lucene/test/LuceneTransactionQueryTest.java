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

    var vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());
    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");
    Assert.assertEquals(0, vertices.stream().count());
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    var vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    var result = vertices.next();
    db.begin();

    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, index.getInternal().size(db));

    doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "abc");

    db.delete(result.getIdentity().get());

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Collection coll;
    try (var rids = index.getInternal().getRids(db, "abc")) {
      coll = rids.collect(Collectors.toList());
    }

    Assert.assertEquals(0, vertices.stream().count());
    Assert.assertEquals(0, coll.size());

    var iterator = coll.iterator();
    var i = 0;
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

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    var c1 = db.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(db);

    db.begin();
    Assert.assertEquals(0, index.getInternal().size(db));

    var doc = ((EntityImpl) db.newEntity("c1"));
    doc.field("p1", "update");

    db.save(doc);

    var vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));

    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Collection coll;
    try (var stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    var res = vertices.next();
    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, coll.size());
    Assert.assertEquals(1, index.getInternal().size(db));

    db.begin();

    var record = db.bindToSession(res.getEntity().get());
    record.setProperty("p1", "removed");
    db.save(record);

    vertices = db.query("select from C1 where p1 lucene \"update\" ");
    try (var stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(0, vertices.stream().count());
    Assert.assertEquals(0, coll.size());

    var iterator = coll.iterator();
    var i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i++;
    }
    Assert.assertEquals(0, i);

    Assert.assertEquals(1, index.getInternal().size(db));

    vertices = db.query("select from C1 where p1 lucene \"removed\"");
    try (var stream = index.getInternal().getRids(db, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
  }

  @Test
  public void txUpdateTestComplex() {

    var index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    var c1 = db.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(db);

    db.begin();
    Assert.assertEquals(0, index.getInternal().size(db));

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

    var vertices = db.query("select from C1 where p1 lucene \"abc\"");
    Collection coll;
    try (var stream = index.getInternal().getRids(db, "abc")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    var iterator = coll.iterator();
    var i = 0;
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

    vertices = db.query("select from C1 where p1 lucene \"removed\" ");
    try (var stream = index.getInternal().getRids(db, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(2, vertices.stream().count());

    Assert.assertEquals(2, index.getInternal().size(db));
  }
}
