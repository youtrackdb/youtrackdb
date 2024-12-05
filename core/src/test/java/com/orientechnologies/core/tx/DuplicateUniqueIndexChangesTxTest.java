/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 */

package com.orientechnologies.core.tx;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.storage.YTRecordDuplicatedException;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DuplicateUniqueIndexChangesTxTest extends DBTestBase {

  private OIndex index;

  public void beforeTest() throws Exception {
    super.beforeTest();
    final YTClass class_ = db.getMetadata().getSchema().createClass("Person");
    index =
        class_
            .createProperty(db, "name", YTType.STRING)
            .createIndex(db, YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
  }

  @Test
  public void testDuplicateNullsOnCreate() {
    db.begin();

    // saved persons will have null name
    final YTEntityImpl person1 = db.newInstance("Person");
    db.save(person1);
    final YTEntityImpl person2 = db.newInstance("Person");
    db.save(person2);
    final YTEntityImpl person3 = db.newInstance("Person");
    db.save(person3);

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  private YTEntityImpl fetchDocumentFromIndex(String o) {
    try (Stream<YTRID> stream = index.getInternal().getRids(db, o)) {
      return (YTEntityImpl) stream.findFirst().map(YTRID::getRecord).orElse(null);
    }
  }

  @Test
  public void testDuplicateNullsOnUpdate() {
    db.begin();
    YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);

    // saved persons will have null name
    person1.field("name", (Object) null).save();
    person2.field("name", (Object) null).save();
    person3.field("name", (Object) null).save();

    // change names back to unique swapped
    person1.field("name", "Name2").save();
    person2.field("name", "Name1").save();
    person3.field("name", "Name3").save();

    // and again
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnCreate() {
    db.begin();

    // saved persons will have same name
    final YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "same");
    db.save(person3);

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnUpdate() {
    db.begin();
    YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    db.begin();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);

    // saved persons will have same name
    person1.field("name", "same").save();
    person2.field("name", "same").save();
    person3.field("name", "same").save();

    // change names back to unique in reverse order
    person3.field("name", "Name3").save();
    person2.field("name", "Name2").save();
    person1.field("name", "Name1").save();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnCreateDelete() {
    db.begin();

    // saved persons will have same name
    final YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "same");
    db.save(person3);
    final YTEntityImpl person4 = db.newInstance("Person");
    person4.field("name", "same");
    db.save(person4);

    person1.delete();
    person2.field("name", "Name2").save();
    person3.delete();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));
  }

  @Test
  public void testDuplicateValuesOnUpdateDelete() {
    db.begin();
    YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    YTEntityImpl person4 = db.newInstance("Person");
    person4.field("name", "Name4");
    db.save(person4);
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);
    person4 = db.bindToSession(person4);

    person1.delete();
    person2.field("name", "same").save();
    person3.delete();
    person4.field("name", "same").save();
    person2.field("name", "Name2").save();

    // should not throw YTRecordDuplicatedException exception
    db.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));

    db.begin();
    person2 = db.bindToSession(person2);
    person4 = db.bindToSession(person4);

    person2.delete();
    person4.delete();
    db.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("Name2"));
    Assert.assertNull(fetchDocumentFromIndex("same"));
  }

  @Test(expected = YTRecordDuplicatedException.class)
  public void testDuplicateCreateThrows() {
    db.begin();
    YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    YTEntityImpl person2 = db.newInstance("Person");
    db.save(person2);
    YTEntityImpl person3 = db.newInstance("Person");
    db.save(person3);
    YTEntityImpl person4 = db.newInstance("Person");
    person4.field("name", "Name1");
    db.save(person4);
    //    Assert.assertThrows(YTRecordDuplicatedException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        db.commit();
    //      }
    //    });
    db.commit();
  }

  @Test(expected = YTRecordDuplicatedException.class)
  public void testDuplicateUpdateThrows() {
    db.begin();
    YTEntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    YTEntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    YTEntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    YTEntityImpl person4 = db.newInstance("Person");
    person4.field("name", "Name4");
    db.save(person4);
    db.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    db.begin();
    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);
    person4 = db.bindToSession(person4);

    person1.field("name", "Name1").save();
    person2.field("name", (Object) null).save();
    person3.field("name", "Name1").save();
    person4.field("name", (Object) null).save();
    db.commit();
  }
}
