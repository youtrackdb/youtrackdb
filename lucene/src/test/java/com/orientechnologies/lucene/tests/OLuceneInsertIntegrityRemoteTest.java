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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", OType.STRING);
    //noinspection deprecation
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  @Ignore
  public void testInsertUpdateWithIndex() throws Exception {
    db.getMetadata().reload();
    OSchema schema = db.getMetadata().getSchema();

    ODocument doc = new ODocument("City");
    doc.field("name", "Rome");

    db.begin();
    db.save(doc);
    db.commit();
    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

    Collection<?> coll;
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);

    doc = db.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "Rome");

    db.begin();
    doc.field("name", "London");
    db.save(doc);
    db.commit();

    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);

    doc = db.load((ORID) coll.iterator().next());
    Assert.assertEquals(doc.field("name"), "London");

    db.begin();
    doc.field("name", "Berlin");
    db.save(doc);
    db.commit();

    doc = db.load(doc.getIdentity(), null, false);
    Assert.assertEquals(doc.field("name"), "Berlin");

    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(idx.getInternal().size(db), 1);
    Assert.assertEquals(coll.size(), 1);

    Thread.sleep(1000);

    doc = db.load(doc.getIdentity(), null, false);

    Assert.assertEquals(doc.field("name"), "Berlin");

    schema = db.getMetadata().getSchema();
    idx = schema.getClass("City").getClassIndex(db, "City.name");

    Assert.assertEquals(idx.getInternal().size(db), 1);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "London")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 0);
    try (Stream<ORID> stream = idx.getInternal().getRids(db, "Berlin")) {
      coll = stream.collect(Collectors.toList());
    }
    Assert.assertEquals(coll.size(), 1);
  }
}
