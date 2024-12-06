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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneTransactionEmbeddedQueryTest {

  public LuceneTransactionEmbeddedQueryTest() {
  }

  @Test
  public void testRollback() {

    @SuppressWarnings("deprecation")
    DatabaseSessionInternal db = new DatabaseDocumentTx("memory:updateTxTest");
    db.create();
    createSchema(db);
    try {
      EntityImpl doc = new EntityImpl("c1");
      doc.field("p1", new String[]{"abc"});
      db.begin();
      db.save(doc);

      String query = "select from C1 where p1 lucene \"abc\" ";
      ResultSet vertices = db.query(query);

      Assert.assertEquals(vertices.stream().count(), 1);
      db.rollback();

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.query(query);
      Assert.assertEquals(vertices.stream().count(), 0);
    } finally {
      db.drop();
    }
  }

  private static void createSchema(DatabaseSession db) {
    final SchemaClass c1 = db.createVertexClass("C1");
    c1.createProperty(db, "p1", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    c1.createIndex(db, "C1.p1", "FULLTEXT", null, null, "LUCENE", new String[]{"p1"});
  }

  @Test
  public void txRemoveTest() {
    @SuppressWarnings("deprecation")
    DatabaseSessionInternal db = new DatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {
      db.begin();

      EntityImpl doc = new EntityImpl("c1");
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

      query = "select from C1 where p1 lucene \"abc\" ";
      vertices = db.query(query);

      Assert.assertEquals(1, vertices.stream().count());

      Assert.assertEquals(1, index.getInternal().size(db));
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }

  @Test
  public void txUpdateTest() {

    @SuppressWarnings("deprecation")
    DatabaseSessionInternal db = new DatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {

      Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

      db.begin();
      Assert.assertEquals(0, index.getInternal().size(db));

      EntityImpl doc = new EntityImpl("c1");
      doc.field("p1", new String[]{"update removed", "update fixed"});

      db.save(doc);

      String query = "select from C1 where p1 lucene \"update\" ";
      ResultSet vertices = db.query(query);

      Assert.assertEquals(vertices.stream().count(), 1);

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

      Assert.assertEquals(vertices.stream().count(), 1);
      Assert.assertEquals(coll.size(), 1);

      Iterator iterator = coll.iterator();
      int i = 0;
      while (iterator.hasNext()) {
        iterator.next();
        i++;
      }
      Assert.assertEquals(i, 1);

      Assert.assertEquals(1, index.getInternal().size(db));

      query = "select from C1 where p1 lucene \"update\"";
      vertices = db.query(query);

      try (Stream<RID> stream = index.getInternal().getRids(db, "update")) {
        coll = stream.collect(Collectors.toList());
      }
      Assert.assertEquals(coll.size(), 1);

      Assert.assertEquals(vertices.stream().count(), 1);

      db.rollback();

      query = "select from C1 where p1 lucene \"update\" ";
      vertices = db.query(query);

      Assert.assertEquals(1, vertices.stream().count());

      Assert.assertEquals(2, index.getInternal().size(db));
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }

  @Test
  public void txUpdateTestComplex() {

    @SuppressWarnings("deprecation")
    DatabaseSessionInternal db = new DatabaseDocumentTx("memory:updateTxTest");
    //noinspection deprecation
    db.create();
    createSchema(db);
    try {
      Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "C1.p1");

      Assert.assertEquals(0, index.getInternal().size(db));

      db.begin();

      EntityImpl doc = new EntityImpl("c1");
      doc.field("p1", new String[]{"abc"});

      EntityImpl doc1 = new EntityImpl("c1");
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
    } finally {
      //noinspection deprecation
      db.drop();
    }
  }
}
