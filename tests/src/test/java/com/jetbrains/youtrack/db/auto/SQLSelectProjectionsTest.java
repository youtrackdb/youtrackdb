/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLSelectProjectionsTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLSelectProjectionsTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    generateGraphData();
    generateProfiles();
  }

  @Test
  public void queryProjectionOk() {
    List<EntityImpl> result =
        db
            .command(
                "select nick, followings, followers from Profile where nick is defined and"
                    + " followings is defined and followers is defined")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      String[] colNames = d.fieldNames();
      Assert.assertEquals(colNames.length, 3, "document: " + d);
      Assert.assertEquals(colNames[0], "nick", "document: " + d);
      Assert.assertEquals(colNames[1], "followings", "document: " + d);
      Assert.assertEquals(colNames[2], "followers", "document: " + d);

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionObjectLevel() {
    var result =
        db.query("select nick, followings, followers from Profile").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 3);
      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionLinkedAndFunction() {
    List<EntityImpl> result =
        db
            .query(
                "select name.toUpperCase(Locale.ENGLISH), address.city.country.name from"
                    + " Profile")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      if (d.field("name") != null) {
        Assert.assertEquals(
            ((String) d.field("name")).toUpperCase(Locale.ENGLISH), d.field("name"));
      }

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSameFieldTwice() {
    List<EntityImpl> result =
        db
            .query(
                "select name, name.toUpperCase(Locale.ENGLISH) as name2 from Profile where name is"
                    + " not null")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertNotNull(d.field("name"));
      Assert.assertNotNull(d.field("name2"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionStaticValues() {
    List<EntityImpl> result =
        db
            .query(
                "select location.city.country.name as location, address.city.country.name as"
                    + " address from Profile where location.city.country.name is not null")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {

      Assert.assertNotNull(d.field("location"));
      Assert.assertNull(d.field("address"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionPrefixAndAppend() {
    List<EntityImpl> result =
        executeQuery(
            "select *, name.prefix('Mr. ').append(' ').append(surname).append('!') as test"
                + " from Profile where name is not null");

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertEquals(
          d.field("test").toString(), "Mr. " + d.field("name") + " " + d.field("surname") + "!");

      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionFunctionsAndFieldOperators() {
    List<EntityImpl> result =
        executeQuery(
            "select name.append('.').prefix('Mr. ') as name from Profile where name is not"
                + " null");

    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertTrue(d.field("name").toString().startsWith("Mr. "));
      Assert.assertTrue(d.field("name").toString().endsWith("."));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSimpleValues() {
    List<EntityImpl> result = executeQuery("select 10, 'ciao' from Profile LIMIT 1");

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertEquals(((Integer) d.field("10")).intValue(), 10);
      Assert.assertEquals(d.field("'ciao'"), "ciao");

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionJSON() {
    List<EntityImpl> result = executeQuery("select @this.toJson() as json from Profile");

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("json"));

      new EntityImpl(null).fromJSON((String) d.field("json"));
    }
  }

  public void queryProjectionRid() {
    List<EntityImpl> result = executeQuery("select @rid as rid FROM V");
    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("rid"));

      final RecordId rid = d.field("rid", RID.class);
      Assert.assertTrue(rid.isValid());
    }
  }

  public void queryProjectionOrigin() {
    List<EntityImpl> result = executeQuery("select @raw as raw FROM V");
    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("raw"));
    }
  }

  public void queryProjectionEval() {
    List<EntityImpl> result = executeQuery("select eval('1 + 4') as result");
    Assert.assertEquals(result.size(), 1);

    for (EntityImpl d : result) {
      Assert.assertEquals(d.<Object>field("result"), 5);
    }
  }

  public void queryProjectionContextArray() {
    List<EntityImpl> result =
        executeQuery("select $a[0] as a0, $a as a from V let $a = outE() where outE().size() > 0");
    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Assert.assertTrue(d.containsField("a"));
      Assert.assertTrue(d.containsField("a0"));

      final EntityImpl a0doc = d.field("a0");
      final EntityImpl firstADoc =
          d.<Iterable<Identifiable>>field("a").iterator().next().getRecord(db);

      Assert.assertTrue(
          EntityHelper.hasSameContentOf(a0doc, db, firstADoc, db, null));
    }
  }

  public void ifNullFunction() {
    List<EntityImpl> result = executeQuery("SELECT ifnull('a', 'b') as ifnull");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "a");

    result = executeQuery("SELECT ifnull('a', 'b', 'c') as ifnull");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "c");

    result = executeQuery("SELECT ifnull(null, 'b') as ifnull");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "b");
  }

  public void setAggregation() {
    var result = executeQuery("SELECT set(name) as set from OUser");
    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertTrue(MultiValue.isMultiValue(d.<Object>field("set")));
      Assert.assertTrue(MultiValue.getSize(d.field("set")) <= 3);
    }
  }

  public void projectionWithNoTarget() {
    List<EntityImpl> result = executeQuery("select 'Ay' as a , 'bEE'");
    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("'bEE'"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b");
    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("'bEE'"), "bEE");
    }
  }

  @Test
  public void testSelectExcludeFunction() {
    try {
      db.createClass("A");
      db.createClass("B");

      db.begin();
      var rootElement = db.newInstance("A");
      var childElement = db.newInstance("B");

      rootElement.setProperty("a", "a");
      rootElement.setProperty("b", "b");

      childElement.setProperty("c", "c");
      childElement.setProperty("d", "d");
      childElement.setProperty("e", "e");

      rootElement.setProperty("child", childElement, PropertyType.LINK);

      rootElement.save();
      db.commit();

      List<EntityImpl> res =
          executeQuery("select a,b, child.exclude('d') as child from " + rootElement.getIdentity());

      Assert.assertNotNull(res.get(0).field("a"));
      Assert.assertNotNull(res.get(0).field("b"));
      Assert.assertNotNull(res.get(0).<EntityImpl>field("child").field("c"));
      Assert.assertNull(res.get(0).<EntityImpl>field("child").field("d"));
      Assert.assertNotNull(res.get(0).<EntityImpl>field("child").field("e"));
    } finally {
      db.command("drop class A").close();
      db.command("drop class B").close();
    }
  }

  @Test
  public void testSimpleExpandExclude() {
    try {
      db.createClass("A");
      db.createClass("B");

      db.begin();
      var rootElement = db.newInstance("A");
      rootElement.setProperty("a", "a");
      rootElement.setProperty("b", "b");

      var childElement = db.newInstance("B");
      childElement.setProperty("c", "c");
      childElement.setProperty("d", "d");
      childElement.setProperty("e", "e");

      rootElement.setProperty("child", childElement, PropertyType.LINK);
      childElement.setProperty("root", List.of(rootElement), PropertyType.LINKLIST);

      rootElement.save();
      db.commit();

      List<EntityImpl> res =
          executeQuery(
              "select child.exclude('d') as link from (select expand(root) from "
                  + childElement.getIdentity()
                  + " )");
      Assert.assertEquals(res.size(), 1);

      EntityImpl root = res.get(0);
      Assert.assertNotNull(root.field("link"));

      Assert.assertNull(root.<EntityImpl>field("link").field("d"));
      Assert.assertNotNull(root.<EntityImpl>field("link").field("c"));
      Assert.assertNotNull(root.<EntityImpl>field("link").field("e"));

    } finally {
      db.command("drop class A").close();
      db.command("drop class B").close();
    }
  }

  @Test
  public void testTempRIDsAreNotRecycledInResultSet() {
    final List<EntityImpl> resultset =
        executeQuery("select name, $l as l from OUser let $l = (select name from OuSer)");

    Assert.assertNotNull(resultset);

    Set<RID> rids = new HashSet<>();
    for (Identifiable d : resultset) {
      final RID rid = d.getIdentity();
      Assert.assertFalse(rids.contains(rid));

      rids.add(rid);

      final List<Identifiable> embeddedList = ((EntityImpl) d.getRecord(db)).field("l");
      Assert.assertNotNull(embeddedList);
      Assert.assertFalse(embeddedList.isEmpty());

      for (Identifiable embedded : embeddedList) {
        if (embedded != null) {
          final RID embeddedRid = embedded.getIdentity();

          Assert.assertFalse(rids.contains(embeddedRid));
          rids.add(rid);
        }
      }
    }
  }
}
