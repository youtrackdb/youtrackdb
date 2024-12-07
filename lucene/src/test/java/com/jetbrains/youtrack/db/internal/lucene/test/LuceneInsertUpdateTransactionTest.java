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
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserIml;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertUpdateTransactionTest extends BaseLuceneTest {

  public LuceneInsertUpdateTransactionTest() {
    super();
  }

  @Before
  public void init() {
    Schema schema = db.getMetadata().getSchema();

    SchemaClass oClass = schema.createClass("City");
    oClass.createProperty(db, "name", PropertyType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() {

    Schema schema = db.getMetadata().getSchema();
    db.begin();
    EntityImpl doc = new EntityImpl("City");
    doc.field("name", "Rome");
    db.save(doc);

    Index idx = schema.getClass("City").getClassIndex(db, "City.name");
    Assert.assertNotNull(idx);

    Collection<?> coll;
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(coll.size(), 1);
    db.rollback();
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    db.begin();
    doc = new EntityImpl("City");
    doc.field("name", "Rome");
    db.save(doc);

    SecurityUserIml user = new SecurityUserIml(db, "test", "test");
    db.save(user.getDocument(db));

    db.commit();
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);
  }
}
