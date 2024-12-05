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

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertUpdateTest extends BaseLuceneTest {

  public LuceneInsertUpdateTest() {
    super();
  }

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testInsertUpdateWithIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("City");
    doc.field("name", "Rome");

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
