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

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
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
// Renable when solved killing issue
public class LuceneInsertIntegrityRemoteTest extends BaseLuceneTest {

  @Before
  public void init() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", PropertyType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testInsertUpdateWithIndex() throws Exception {

    db.getMetadata().reload();
    Schema schema = db.getMetadata().getSchema();

    EntityImpl doc = ((EntityImpl) db.newEntity("City"));
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
    Index idx = db.getClassInternal("City").getClassIndex(db, "City.name");

    Collection<?> coll;
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());

    doc = db.load((RID) coll.iterator().next());
    Assert.assertEquals("Rome", doc.field("name"));

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("name", "London");
    db.save(doc);
    db.commit();

    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<RID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());

    doc = db.load((RID) coll.iterator().next());
    Assert.assertEquals("London", doc.field("name"));

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("name", "Berlin");
    db.save(doc);
    db.commit();

    doc = db.load(doc.getIdentity());
    Assert.assertEquals("Berlin", doc.field("name"));

    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<RID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }

    db.begin();
    Assert.assertEquals(1, idx.getInternal().size(db));
    Assert.assertEquals(1, coll.size());
    db.commit();

    Thread.sleep(1000);

    // FIXME
    //    initDB();
    //
    doc = db.load(doc.getIdentity());

    Assert.assertEquals("Berlin", doc.field("name"));

    schema = db.getMetadata().getSchema();
    idx = db.getClassInternal("City").getClassIndex(db, "City.name");

    Assert.assertEquals(1, idx.getInternal().size(db));
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<RID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());
  }
}
