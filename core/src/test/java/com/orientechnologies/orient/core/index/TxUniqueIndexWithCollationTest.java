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

package com.orientechnologies.orient.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class TxUniqueIndexWithCollationTest extends DBTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.getMetadata()
        .getSchema()
        .createClass("user")
        .createProperty(db, "name", YTType.STRING)
        .setCollate(db, "ci")
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    db.begin();
    YTEntity one = db.newEntity("user");
    one.setProperty("name", "abc");
    db.save(one);

    YTEntity two = db.newEntity("user");
    two.setProperty("name", "aby");
    db.save(two);

    YTEntity three = db.newEntity("user");
    three.setProperty("name", "abz");
    db.save(three);
    db.commit();
  }

  @Test
  public void testSubstrings() {
    db.begin();

    db.command("update user set name='abd' where name='Aby'").close();

    final YTResultSet r = db.command("select * from user where name like '%B%' order by name");
    assertEquals("abc", r.next().getProperty("name"));
    assertEquals("abd", r.next().getProperty("name"));
    assertEquals("abz", r.next().getProperty("name"));
    assertFalse(r.hasNext());
    r.close();

    db.commit();
  }

  @Test
  public void testRange() {
    db.begin();

    db.command("update user set name='Abd' where name='Aby'").close();

    final YTResultSet r = db.command("select * from user where name >= 'abd' order by name");
    assertEquals("Abd", r.next().getProperty("name"));
    assertEquals("abz", r.next().getProperty("name"));
    assertFalse(r.hasNext());

    db.commit();
  }

  @Test
  public void testIn() {
    db.begin();

    db.command("update user set name='abd' where name='Aby'").close();

    final List<YTDocument> r =
        db.query("select * from user where name in ['Abc', 'Abd', 'Abz'] order by name").stream()
            .map(x -> ((YTDocument) (x.toEntity())))
            .toList();
    assertEquals(3, r.size());
    assertEquals("abc", r.get(0).field("name"));
    assertEquals("abd", r.get(1).field("name"));
    assertEquals("abz", r.get(2).field("name"));

    db.commit();
  }
}
