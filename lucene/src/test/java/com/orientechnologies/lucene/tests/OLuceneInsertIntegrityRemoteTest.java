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
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
// Renable when solved killing issue
public class OLuceneInsertIntegrityRemoteTest extends OLuceneBaseTest {

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    //noinspection deprecation
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  @Ignore
  public void testInsertUpdateWithIndex() throws Exception {
    db.getMetadata().reload();
    YTSchema schema = db.getMetadata().getSchema();

    YTEntityImpl doc = new YTEntityImpl("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

    Collection<?> coll;
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());

    doc = db.load((YTRID) coll.iterator().next());
    Assert.assertEquals("Rome", doc.field("name"));

    db.begin();
    doc.field("name", "London");
    db.save(doc);
    db.commit();

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());

    doc = db.load((YTRID) coll.iterator().next());
    Assert.assertEquals("London", doc.field("name"));

    db.begin();
    doc.field("name", "Berlin");
    db.save(doc);
    db.commit();

    doc = db.load(doc.getIdentity());
    Assert.assertEquals("Berlin", doc.field("name"));

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, idx.getInternal().size(db));
    Assert.assertEquals(1, coll.size());

    Thread.sleep(1000);

    doc = db.load(doc.getIdentity());

    Assert.assertEquals("Berlin", doc.field("name"));

    schema = db.getMetadata().getSchema();
    idx = schema.getClass("City").getClassIndex(db, "City.name");

    Assert.assertEquals(1, idx.getInternal().size(db));
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(0, coll.size());
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(1, coll.size());
  }
}
