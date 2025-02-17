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

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionQueryTest extends BaseLuceneTest {

  @Before
  public void init() {

    final var c1 = session.createVertexClass("C1");
    c1.createProperty(session, "p1", PropertyType.STRING);
    c1.createIndex(session, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {

    var doc = ((EntityImpl) session.newEntity("c1"));
    doc.field("p1", "abc");
    session.begin();
    session.save(doc);

    var vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());
    session.rollback();

    vertices = session.query("select from C1 where p1 lucene \"abc\" ");
    Assert.assertEquals(0, vertices.stream().count());
  }

  @Test
  public void txRemoveTest() {
    session.begin();

    var doc = ((EntityImpl) session.newEntity("c1"));
    doc.field("p1", "abc");

    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "C1.p1");

    session.save(doc);

    var vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(session));
    session.commit();

    vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    var result = vertices.next();
    session.begin();

    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, index.getInternal().size(session));

    doc = ((EntityImpl) session.newEntity("c1"));
    doc.field("p1", "abc");

    session.delete(result.castToRecord());

    vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    Collection coll;
    try (var rids = index.getInternal().getRids(session, "abc")) {
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
    Assert.assertEquals(0, index.getInternal().size(session));

    session.rollback();

    vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(session));
  }

  @Test
  public void txUpdateTest() {

    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "C1.p1");
    var c1 = session.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(session);

    session.begin();
    Assert.assertEquals(0, index.getInternal().size(session));

    var doc = ((EntityImpl) session.newEntity("c1"));
    doc.field("p1", "update");

    session.save(doc);

    var vertices = session.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(session));

    session.commit();

    vertices = session.query("select from C1 where p1 lucene \"update\" ");

    Collection coll;
    try (var stream = index.getInternal().getRids(session, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    var res = vertices.next();
    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, coll.size());
    Assert.assertEquals(1, index.getInternal().size(session));

    session.begin();

    var record = session.bindToSession(res.castToEntity());
    record.setProperty("p1", "removed");
    session.save(record);

    vertices = session.query("select from C1 where p1 lucene \"update\" ");
    try (var stream = index.getInternal().getRids(session, "update")) {
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

    Assert.assertEquals(1, index.getInternal().size(session));

    vertices = session.query("select from C1 where p1 lucene \"removed\"");
    try (var stream = index.getInternal().getRids(session, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    session.rollback();

    vertices = session.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(session));
  }

  @Test
  public void txUpdateTestComplex() {

    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "C1.p1");
    var c1 = session.getMetadata().getSchema().getClassInternal("C1");
    c1.truncate(session);

    session.begin();
    Assert.assertEquals(0, index.getInternal().size(session));

    var doc = ((EntityImpl) session.newEntity("c1"));
    doc.field("p1", "abc");

    var doc1 = ((EntityImpl) session.newEntity("c1"));
    doc1.field("p1", "abc");

    session.save(doc1);
    session.save(doc);

    session.commit();

    session.begin();

    doc = session.bindToSession(doc);
    doc.field("p1", "removed");
    session.save(doc);

    var vertices = session.query("select from C1 where p1 lucene \"abc\"");
    Collection coll;
    try (var stream = index.getInternal().getRids(session, "abc")) {
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
    Assert.assertEquals(2, index.getInternal().size(session));

    vertices = session.query("select from C1 where p1 lucene \"removed\" ");
    try (var stream = index.getInternal().getRids(session, "removed")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(1, vertices.stream().count());
    Assert.assertEquals(1, coll.size());

    session.rollback();

    vertices = session.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(2, vertices.stream().count());

    Assert.assertEquals(2, index.getInternal().size(session));
  }
}
