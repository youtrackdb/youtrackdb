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

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertUpdateSingleDocumentTransactionTest extends LuceneBaseTest {

  @Before
  public void init() {
    Schema schema = db.getMetadata().getSchema();

    var oClass = schema.createClass("City");
    oClass.createProperty(db, "name", PropertyType.STRING);
    //noinspection EmptyTryBlock
    try (var command =
        db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE")) {
    }
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() {

    Schema schema = db.getMetadata().getSchema();
    db.begin();
    var doc = ((EntityImpl) db.newEntity("City"));
    doc.field("name", "");
    var doc1 = ((EntityImpl) db.newEntity("City"));
    doc1.field("name", "");
    doc = db.save(doc);
    doc1 = db.save(doc1);
    db.commit();
    db.begin();
    doc = db.load(doc.getIdentity());
    doc1 = db.load(doc1.getIdentity());
    doc.field("name", "Rome");
    doc1.field("name", "Rome");
    db.save(doc);
    db.save(doc1);
    db.commit();

    var indexManager = db.getMetadata().getIndexManager();
    var idx = indexManager.getIndex("City.name");
    Collection<?> coll;
    try (var stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.toList();
    }

    db.begin();
    Assert.assertEquals(2, coll.size());
    Assert.assertEquals(2, idx.getInternal().size(db));
    db.commit();
  }
}
