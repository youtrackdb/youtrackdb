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

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
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
public class OLuceneInsertUpdateTest extends OLuceneBaseTest {

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
  public void testInsertUpdateWithIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    YTDocument doc = new YTDocument("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
    db.begin();
    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");
    Collection<?> coll;
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);

    YTIdentifiable next = (YTIdentifiable) coll.iterator().next();
    doc = db.load(next.getIdentity());
    Assert.assertEquals(doc.field("name"), "Rome");

    doc.field("name", "London");

    db.save(doc);
    db.commit();
    db.begin();
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);

    next = (YTIdentifiable) coll.iterator().next();
    doc = db.load(next.getIdentity());
    Assert.assertEquals(doc.field("name"), "London");

    doc.field("name", "Berlin");

    db.save(doc);
    db.commit();

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }

    Assert.assertEquals(coll.size(), 0);
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);
  }
}
