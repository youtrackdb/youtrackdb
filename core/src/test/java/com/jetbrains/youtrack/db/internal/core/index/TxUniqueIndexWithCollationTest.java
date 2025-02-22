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

package com.jetbrains.youtrack.db.internal.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

/**
 *
 */
public class TxUniqueIndexWithCollationTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    session.getMetadata()
        .getSchema()
        .createClass("user")
        .createProperty(session, "name", PropertyType.STRING)
        .setCollate(session, "ci")
        .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);

    session.begin();
    var one = session.newEntity("user");
    one.setProperty("name", "abc");

    var two = session.newEntity("user");
    two.setProperty("name", "aby");

    var three = session.newEntity("user");
    three.setProperty("name", "abz");
    session.commit();
  }

  @Test
  public void testSubstrings() {
    session.begin();

    session.command("update user set name='abd' where name='Aby'").close();

    final var r = session.command("select * from user where name like '%B%' order by name");
    assertEquals("abc", r.next().getProperty("name"));
    assertEquals("abd", r.next().getProperty("name"));
    assertEquals("abz", r.next().getProperty("name"));
    assertFalse(r.hasNext());
    r.close();

    session.commit();
  }

  @Test
  public void testRange() {
    session.begin();

    session.command("update user set name='Abd' where name='Aby'").close();

    final var r = session.command("select * from user where name >= 'abd' order by name");
    assertEquals("Abd", r.next().getProperty("name"));
    assertEquals("abz", r.next().getProperty("name"));
    assertFalse(r.hasNext());

    session.commit();
  }

  @Test
  public void testIn() {
    session.begin();

    session.command("update user set name='abd' where name='Aby'").close();

    final var r =
        session.query("select * from user where name in ['Abc', 'Abd', 'Abz'] order by name")
            .stream()
            .map(x -> ((EntityImpl) (x.asEntity())))
            .toList();
    assertEquals(3, r.size());
    assertEquals("abc", r.get(0).field("name"));
    assertEquals("abd", r.get(1).field("name"));
    assertEquals("abz", r.get(2).field("name"));

    session.commit();
  }
}
