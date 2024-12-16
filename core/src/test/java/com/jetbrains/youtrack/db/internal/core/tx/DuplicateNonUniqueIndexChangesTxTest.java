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

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DuplicateNonUniqueIndexChangesTxTest extends DbTestBase {

  private Index index;

  public void beforeTest() throws Exception {
    super.beforeTest();
    final SchemaClass class_ = db.getMetadata().getSchema().createClass("Person");
    var indexName =
        class_
            .createProperty(db, "name", PropertyType.STRING)
            .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    index = db.getIndex(indexName);
  }

  @Test
  public void testDuplicateNullsOnCreate() {
    db.begin();

    // saved persons will have null name
    final EntityImpl person1 = db.newInstance("Person");
    db.save(person1);
    final EntityImpl person2 = db.newInstance("Person");
    db.save(person2);
    final EntityImpl person3 = db.newInstance("Person");
    db.save(person3);

    // change some names
    person3.field("name", "Name3").save();

    db.commit();

    // verify index state
    assertRids(null, person1, person2);
    assertRids("Name3", person3);
  }

  @Test
  public void testDuplicateNullsOnUpdate() {
    db.begin();
    EntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    EntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    EntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    db.commit();

    // verify index state
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);

    db.begin();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);

    // saved persons will have null name
    person1.field("name", (Object) null).save();
    person2.field("name", (Object) null).save();
    person3.field("name", (Object) null).save();

    // change names
    person1.field("name", "Name2").save();
    person2.field("name", "Name1").save();
    person3.field("name", "Name2").save();

    // and again
    person1.field("name", "Name1").save();
    person2.field("name", "Name2").save();

    db.commit();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);

    // verify index state
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2, person3);
    assertRids("Name3");
  }

  @Test
  public void testDuplicateValuesOnCreate() {
    db.begin();

    // saved persons will have same name
    final EntityImpl person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final EntityImpl person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final EntityImpl person3 = db.newInstance("Person");
    person3.field("name", "same");
    db.save(person3);

    // change some names
    person2.field("name", "Name1").save();
    person2.field("name", "Name2").save();
    person3.field("name", "Name2").save();

    db.commit();

    // verify index state
    assertRids("same", person1);
    assertRids("Name1");
    assertRids("Name2", person2, person3);
  }

  @Test
  public void testDuplicateValuesOnUpdate() {
    db.begin();
    EntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    EntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    EntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);
    db.commit();

    // verify index state
    assertRids(null);
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);

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

    db.commit();

    person1 = db.bindToSession(person1);
    person2 = db.bindToSession(person2);
    person3 = db.bindToSession(person3);

    // verify index state
    assertRids("same");
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);
  }

  @Test
  public void testDuplicateValuesOnCreateDelete() {
    db.begin();

    // saved persons will have same name
    final EntityImpl person1 = db.newInstance("Person");
    person1.field("name", "same");
    db.save(person1);
    final EntityImpl person2 = db.newInstance("Person");
    person2.field("name", "same");
    db.save(person2);
    final EntityImpl person3 = db.newInstance("Person");
    person3.field("name", "same");
    db.save(person3);
    final EntityImpl person4 = db.newInstance("Person");
    person4.field("name", "same");
    db.save(person4);

    person1.delete();
    person2.field("name", "Name2").save();
    person3.delete();
    person4.field("name", "Name2").save();

    db.commit();

    // verify index state
    assertRids("Name1");
    assertRids("Name2", person2, person4);
    assertRids("Name3");
    assertRids("Name4");
  }

  @Test
  public void testDuplicateValuesOnUpdateDelete() {
    db.begin();
    EntityImpl person1 = db.newInstance("Person");
    person1.field("name", "Name1");
    db.save(person1);
    EntityImpl person2 = db.newInstance("Person");
    person2.field("name", "Name2");
    db.save(person2);
    EntityImpl person3 = db.newInstance("Person");
    person3.field("name", "Name3");
    db.save(person3);

    EntityImpl person4 = db.newInstance("Person");
    person4.field("name", "Name4");
    db.save(person4);
    db.commit();

    // verify index state
    assertRids("Name1", person1);
    assertRids("Name2", person2);
    assertRids("Name3", person3);
    assertRids("Name4", person4);

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
    person4.field("name", "Name2").save();

    db.commit();

    person2 = db.bindToSession(person2);
    person4 = db.bindToSession(person4);

    // verify index state
    assertRids("same");
    assertRids("Name1");
    assertRids("Name2", person2, person4);
    assertRids("Name3");
    assertRids("Name4");

    db.begin();
    person2 = db.bindToSession(person2);
    person4 = db.bindToSession(person4);

    person2.delete();
    person4.delete();
    db.commit();

    // verify index state
    assertRids("Name2");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testManyManyUpdatesToTheSameKey() {
    final Set<Integer> unseen = new HashSet<Integer>();

    db.begin();
    for (int i = 0; i < FrontendTransactionIndexChangesPerKey.SET_ADD_THRESHOLD * 2; ++i) {
      EntityImpl pers = db.newInstance("Person");
      pers.field("name", "Name");
      pers.field("serial", i);
      db.save(pers);
      unseen.add(i);
    }
    db.commit();

    // verify index state
    try (Stream<RID> stream = index.getInternal().getRids(db, "Name")) {
      stream.forEach(
          (rid) -> {
            final EntityImpl document = db.load(rid);
            unseen.remove(document.<Integer>field("serial"));
          });
    }
    Assert.assertTrue(unseen.isEmpty());
  }

  @SuppressWarnings("unchecked")
  private void assertRids(String indexKey, Identifiable... rids) {
    final Set<RID> actualRids;
    try (Stream<RID> stream = index.getInternal().getRids(db, indexKey)) {
      actualRids = stream.collect(Collectors.toSet());
    }
    Assert.assertEquals(actualRids, new HashSet<Object>(Arrays.asList(rids)));
  }
}
