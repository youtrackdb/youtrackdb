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

package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DuplicateUniqueIndexChangesTxTest extends DbTestBase {

  private Index index;

  public void beforeTest() throws Exception {
    super.beforeTest();
    final var class_ = session.getMetadata().getSchema().createClass("Person");
    var indexName =
        class_
            .createProperty(session, "name", PropertyType.STRING)
            .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);
    index = session.getIndex(indexName);
  }

  @Test
  public void testDuplicateNullsOnCreate() {
    session.begin();

    // saved persons will have null name
    final EntityImpl person1 = session.newInstance("Person");
    session.save(person1);
    final EntityImpl person2 = session.newInstance("Person");
    session.save(person2);
    final EntityImpl person3 = session.newInstance("Person");
    session.save(person3);

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  private EntityImpl fetchDocumentFromIndex(String o) {
    try (var stream = index.getInternal().getRids(session, o)) {
      return (EntityImpl) stream.findFirst().map(rid -> rid.getRecord(session)).orElse(null);
    }
  }

  @Test
  public void testDuplicateNullsOnUpdate() {
    session.begin();
    EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "Name1");
    session.save(person1);
    EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "Name2");
    session.save(person2);
    EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "Name3");
    session.save(person3);
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    session.begin();

    person1 = session.bindToSession(person1);
    person2 = session.bindToSession(person2);
    person3 = session.bindToSession(person3);

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

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex(null));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnCreate() {
    session.begin();

    // saved persons will have same name
    final EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "same");
    session.save(person1);
    final EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "same");
    session.save(person2);
    final EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "same");
    session.save(person3);

    // change names to unique
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name3").save();

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnUpdate() {
    session.begin();
    EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "Name1");
    session.save(person1);
    EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "Name2");
    session.save(person2);
    EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "Name3");
    session.save(person3);
    session.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));

    session.begin();

    person1 = session.bindToSession(person1);
    person2 = session.bindToSession(person2);
    person3 = session.bindToSession(person3);

    // saved persons will have same name
    person1.field("name", "same").save();
    person2.field("name", "same").save();
    person3.field("name", "same").save();

    // change names back to unique in reverse order
    person3.field("name", "Name3").save();
    person2.field("name", "Name2").save();
    person1.field("name", "Name1").save();

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("same"));
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
  }

  @Test
  public void testDuplicateValuesOnCreateDelete() {
    session.begin();

    // saved persons will have same name
    final EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "same");
    session.save(person1);
    final EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "same");
    session.save(person2);
    final EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "same");
    session.save(person3);
    final EntityImpl person4 = session.newInstance("Person");
    person4.field("name", "same");
    session.save(person4);

    person1.delete();
    person2.field("name", "Name2").save();
    person3.delete();

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));
  }

  @Test
  public void testDuplicateValuesOnUpdateDelete() {
    session.begin();
    EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "Name1");
    session.save(person1);
    EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "Name2");
    session.save(person2);
    EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "Name3");
    session.save(person3);
    EntityImpl person4 = session.newInstance("Person");
    person4.field("name", "Name4");
    session.save(person4);
    session.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    session.begin();

    person1 = session.bindToSession(person1);
    person2 = session.bindToSession(person2);
    person3 = session.bindToSession(person3);
    person4 = session.bindToSession(person4);

    person1.delete();
    person2.field("name", "same").save();
    person3.delete();
    person4.field("name", "same").save();
    person2.field("name", "Name2").save();

    // should not throw RecordDuplicatedException exception
    session.commit();

    // verify index state
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("same"));

    session.begin();
    person2 = session.bindToSession(person2);
    person4 = session.bindToSession(person4);

    person2.delete();
    person4.delete();
    session.commit();

    // verify index state
    Assert.assertNull(fetchDocumentFromIndex("Name2"));
    Assert.assertNull(fetchDocumentFromIndex("same"));
  }

  @Test(expected = RecordDuplicatedException.class)
  public void testDuplicateCreateThrows() {
    session.begin();
    EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "Name1");
    session.save(person1);
    EntityImpl person2 = session.newInstance("Person");
    session.save(person2);
    EntityImpl person3 = session.newInstance("Person");
    session.save(person3);
    EntityImpl person4 = session.newInstance("Person");
    person4.field("name", "Name1");
    session.save(person4);
    //    Assert.assertThrows(RecordDuplicatedException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        db.commit();
    //      }
    //    });
    session.commit();
  }

  @Test(expected = RecordDuplicatedException.class)
  public void testDuplicateUpdateThrows() {
    session.begin();
    EntityImpl person1 = session.newInstance("Person");
    person1.field("name", "Name1");
    session.save(person1);
    EntityImpl person2 = session.newInstance("Person");
    person2.field("name", "Name2");
    session.save(person2);
    EntityImpl person3 = session.newInstance("Person");
    person3.field("name", "Name3");
    session.save(person3);
    EntityImpl person4 = session.newInstance("Person");
    person4.field("name", "Name4");
    session.save(person4);
    session.commit();

    // verify index state
    Assert.assertEquals(person1, fetchDocumentFromIndex("Name1"));
    Assert.assertEquals(person2, fetchDocumentFromIndex("Name2"));
    Assert.assertEquals(person3, fetchDocumentFromIndex("Name3"));
    Assert.assertEquals(person4, fetchDocumentFromIndex("Name4"));

    session.begin();
    person1 = session.bindToSession(person1);
    person2 = session.bindToSession(person2);
    person3 = session.bindToSession(person3);
    person4 = session.bindToSession(person4);

    person1.field("name", "Name1").save();
    person2.field("name", (Object) null).save();
    person3.field("name", "Name1").save();
    person4.field("name", (Object) null).save();
    session.commit();
  }
}
