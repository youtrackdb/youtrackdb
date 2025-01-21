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

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
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
public class LuceneInsertUpdateTransactionTest extends LuceneBaseTest {

  @Before
  public void init() {
    Schema schema = db.getMetadata().getSchema();

    SchemaClass oClass = schema.createClass("City");
    oClass.createProperty(db, "name", PropertyType.STRING);
    //noinspection EmptyTryBlock
    try (ResultSet command =
        db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE")) {
    }
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() {

    var schema = db.getMetadata().getSchema();
    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("City"));
    doc.field("name", "Rome");
    db.save(doc);

    Index idx = schema.getClassInternal("City").getClassIndex(db, "City.name");
    Assert.assertNotNull(idx);
    Collection<?> coll;
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());
    db.rollback();
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    db.begin();
    doc = ((EntityImpl) db.newEntity("City"));
    doc.field("name", "Rome");
    db.save(doc);

    SecurityUserImpl user = new SecurityUserImpl(db, "test", "test");
    user.save(db);

    db.commit();
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());
  }
}
