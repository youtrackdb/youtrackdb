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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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

    final YTClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", YTType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void testRollback() {

    YTDocument doc = new YTDocument("c1");
    doc.field("p1", "abc");
    db.begin();
    db.save(doc);

    OResultSet vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(vertices.stream().count(), 1);
    db.rollback();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");
    Assert.assertEquals(vertices.stream().count(), 0);
  }

  @Test
  public void txRemoveTest() {
    db.begin();

    YTDocument doc = new YTDocument("c1");
    doc.field("p1", "abc");

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

    db.save(doc);

    OResultSet vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Assert.assertEquals(1, vertices.stream().count());

    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    OResult result = vertices.next();
    db.begin();

    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(1, index.getInternal().size(db));

    doc = new YTDocument("c1");
    doc.field("p1", "abc");

    db.delete(result.getIdentity().get());

    vertices = db.query("select from C1 where p1 lucene \"abc\" ");

    Collection coll;
    try (Stream<YTRID> rids = index.getInternal().getRids(db, "abc")) {
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

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    YTClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate(db);
    } catch (IOException e) {
      e.printStackTrace();
    }

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    YTDocument doc = new YTDocument("c1");
    doc.field("p1", "update");

    db.save(doc);

    OResultSet vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Assert.assertEquals(vertices.stream().count(), 1);

    Assert.assertEquals(index.getInternal().size(db), 1);

    db.commit();

    vertices = db.query("select from C1 where p1 lucene \"update\" ");

    Collection coll;
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "update")) {
      coll = stream.collect(Collectors.toList());
    }

    OResult res = vertices.next();
    Assert.assertFalse(vertices.hasNext());
    Assert.assertEquals(coll.size(), 1);
    Assert.assertEquals(index.getInternal().size(db), 1);

    db.begin();

    YTEntity record = db.bindToSession(res.getElement().get());
    record.setProperty("p1", "removed");
    db.save(record);

    vertices = db.query("select from C1 where p1 lucene \"update\" ");
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "update")) {
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
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "removed")) {
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

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");
    YTClass c1 = db.getMetadata().getSchema().getClass("C1");
    try {
      c1.truncate(db);
    } catch (IOException e) {
      e.printStackTrace();
    }

    db.begin();
    Assert.assertEquals(index.getInternal().size(db), 0);

    YTDocument doc = new YTDocument("c1");
    doc.field("p1", "abc");

    YTDocument doc1 = new YTDocument("c1");
    doc1.field("p1", "abc");

    db.save(doc1);
    db.save(doc);

    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    doc.field("p1", "removed");
    db.save(doc);

    OResultSet vertices = db.query("select from C1 where p1 lucene \"abc\"");
    Collection coll;
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "abc")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(vertices.stream().count(), 1);
    Assert.assertEquals(coll.size(), 1);

    Iterator iterator = coll.iterator();
    int i = 0;
    YTRecordId rid = null;
    while (iterator.hasNext()) {
      rid = (YTRecordId) iterator.next();
      i++;
    }

    Assert.assertEquals(i, 1);
    Assert.assertNotNull(doc1);
    Assert.assertNotNull(rid);
    Assert.assertEquals(doc1.getIdentity().toString(), rid.getIdentity().toString());
    Assert.assertEquals(index.getInternal().size(db), 2);

    vertices = db.query("select from C1 where p1 lucene \"removed\" ");
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "removed")) {
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
