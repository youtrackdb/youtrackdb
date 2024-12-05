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

package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.metadata.security.YTUser;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneInsertUpdateTransactionTest extends OLuceneBaseTest {

  @Before
  public void init() {
    YTSchema schema = db.getMetadata().getSchema();

    YTClass oClass = schema.createClass("City");
    oClass.createProperty(db, "name", YTType.STRING);
    //noinspection EmptyTryBlock
    try (YTResultSet command =
        db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE")) {
    }
  }

  @Test
  public void testInsertUpdateTransactionWithIndex() {

    YTSchema schema = db.getMetadata().getSchema();
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("City");
    doc.field("name", "Rome");
    db.save(doc);

    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");
    Assert.assertNotNull(idx);
    Collection<?> coll;
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);
    db.rollback();
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    db.begin();
    doc = new YTEntityImpl("City");
    doc.field("name", "Rome");
    db.save(doc);

    YTUser user = new YTUser(db, "test", "test");
    db.save(user.getDocument(db));

    db.commit();
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);
  }
}
