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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.ODocumentHelper;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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
public class SQLSelectProjectionsTest extends DocumentDBBaseTest {

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
    List<YTEntityImpl> result =
        database
            .command(
                "select nick, followings, followers from Profile where nick is defined and"
                    + " followings is defined and followers is defined")
            .stream()
            .map(r -> (YTEntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      String[] colNames = d.fieldNames();
      Assert.assertEquals(colNames.length, 3, "document: " + d);
      Assert.assertEquals(colNames[0], "nick", "document: " + d);
      Assert.assertEquals(colNames[1], "followings", "document: " + d);
      Assert.assertEquals(colNames[2], "followers", "document: " + d);

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionObjectLevel() {
    var result =
        database.query("select nick, followings, followers from Profile").stream()
            .map(r -> (YTEntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 3);
      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionLinkedAndFunction() {
    List<YTEntityImpl> result =
        database
            .query(
                "select name.toUpperCase(Locale.ENGLISH), address.city.country.name from"
                    + " Profile")
            .stream()
            .map(r -> (YTEntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      if (d.field("name") != null) {
        Assert.assertEquals(
            ((String) d.field("name")).toUpperCase(Locale.ENGLISH), d.field("name"));
      }

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSameFieldTwice() {
    List<YTEntityImpl> result =
        database
            .query(
                "select name, name.toUpperCase(Locale.ENGLISH) as name2 from Profile where name is"
                    + " not null")
            .stream()
            .map(r -> (YTEntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertNotNull(d.field("name"));
      Assert.assertNotNull(d.field("name2"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionStaticValues() {
    List<YTEntityImpl> result =
        database
            .query(
                "select location.city.country.name as location, address.city.country.name as"
                    + " address from Profile where location.city.country.name is not null")
            .stream()
            .map(r -> (YTEntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {

      Assert.assertNotNull(d.field("location"));
      Assert.assertNull(d.field("address"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionPrefixAndAppend() {
    List<YTEntityImpl> result =
        executeQuery(
            "select *, name.prefix('Mr. ').append(' ').append(surname).append('!') as test"
                + " from Profile where name is not null");

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertEquals(
          d.field("test").toString(), "Mr. " + d.field("name") + " " + d.field("surname") + "!");

      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionFunctionsAndFieldOperators() {
    List<YTEntityImpl> result =
        executeQuery(
            "select name.append('.').prefix('Mr. ') as name from Profile where name is not"
                + " null");

    Assert.assertFalse(result.isEmpty());
    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertTrue(d.field("name").toString().startsWith("Mr. "));
      Assert.assertTrue(d.field("name").toString().endsWith("."));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSimpleValues() {
    List<YTEntityImpl> result = executeQuery("select 10, 'ciao' from Profile LIMIT 1");

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertEquals(((Integer) d.field("10")).intValue(), 10);
      Assert.assertEquals(d.field("'ciao'"), "ciao");

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTEntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionJSON() {
    List<YTEntityImpl> result = executeQuery("select @this.toJson() as json from Profile");

    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("json"));

      new YTEntityImpl().fromJSON((String) d.field("json"));
    }
  }

  public void queryProjectionRid() {
    List<YTEntityImpl> result = executeQuery("select @rid as rid FROM V");
    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("rid"));

      final YTRID rid = d.field("rid", YTRID.class);
      Assert.assertTrue(rid.isValid());
    }
  }

  public void queryProjectionOrigin() {
    List<YTEntityImpl> result = executeQuery("select @raw as raw FROM V");
    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("raw"));
    }
  }

  public void queryProjectionEval() {
    List<YTEntityImpl> result = executeQuery("select eval('1 + 4') as result");
    Assert.assertEquals(result.size(), 1);

    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.<Object>field("result"), 5);
    }
  }

  public void queryProjectionContextArray() {
    List<YTEntityImpl> result =
        executeQuery("select $a[0] as a0, $a as a from V let $a = outE() where outE().size() > 0");
    Assert.assertFalse(result.isEmpty());

    for (YTEntityImpl d : result) {
      Assert.assertTrue(d.containsField("a"));
      Assert.assertTrue(d.containsField("a0"));

      final YTEntityImpl a0doc = d.field("a0");
      final YTEntityImpl firstADoc =
          d.<Iterable<YTIdentifiable>>field("a").iterator().next().getRecord();

      Assert.assertTrue(
          ODocumentHelper.hasSameContentOf(a0doc, database, firstADoc, database, null));
    }
  }

  public void ifNullFunction() {
    List<YTEntityImpl> result = executeQuery("SELECT ifnull('a', 'b') as ifnull");
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
    for (YTEntityImpl d : result) {
      Assert.assertTrue(OMultiValue.isMultiValue(d.<Object>field("set")));
      Assert.assertTrue(OMultiValue.getSize(d.field("set")) <= 3);
    }
  }

  public void projectionWithNoTarget() {
    List<YTEntityImpl> result = executeQuery("select 'Ay' as a , 'bEE'");
    Assert.assertEquals(result.size(), 1);
    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("'bEE'"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b");
    Assert.assertEquals(result.size(), 1);
    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (YTEntityImpl d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("'bEE'"), "bEE");
    }
  }

  @Test
  public void testSelectExcludeFunction() {
    try {
      database.createClass("A");
      database.createClass("B");

      database.begin();
      var rootElement = database.newInstance("A");
      var childElement = database.newInstance("B");

      rootElement.setProperty("a", "a");
      rootElement.setProperty("b", "b");

      childElement.setProperty("c", "c");
      childElement.setProperty("d", "d");
      childElement.setProperty("e", "e");

      rootElement.setProperty("child", childElement, YTType.LINK);

      rootElement.save();
      database.commit();

      List<YTEntityImpl> res =
          executeQuery("select a,b, child.exclude('d') as child from " + rootElement.getIdentity());

      Assert.assertNotNull(res.get(0).field("a"));
      Assert.assertNotNull(res.get(0).field("b"));
      Assert.assertNotNull(res.get(0).<YTEntityImpl>field("child").field("c"));
      Assert.assertNull(res.get(0).<YTEntityImpl>field("child").field("d"));
      Assert.assertNotNull(res.get(0).<YTEntityImpl>field("child").field("e"));
    } finally {
      database.command("drop class A").close();
      database.command("drop class B").close();
    }
  }

  @Test
  public void testSimpleExpandExclude() {
    try {
      database.createClass("A");
      database.createClass("B");

      database.begin();
      var rootElement = database.newInstance("A");
      rootElement.setProperty("a", "a");
      rootElement.setProperty("b", "b");

      var childElement = database.newInstance("B");
      childElement.setProperty("c", "c");
      childElement.setProperty("d", "d");
      childElement.setProperty("e", "e");

      rootElement.setProperty("child", childElement, YTType.LINK);
      childElement.setProperty("root", List.of(rootElement), YTType.LINKLIST);

      rootElement.save();
      database.commit();

      List<YTEntityImpl> res =
          executeQuery(
              "select child.exclude('d') as link from (select expand(root) from "
                  + childElement.getIdentity()
                  + " )");
      Assert.assertEquals(res.size(), 1);

      YTEntityImpl root = res.get(0);
      Assert.assertNotNull(root.field("link"));

      Assert.assertNull(root.<YTEntityImpl>field("link").field("d"));
      Assert.assertNotNull(root.<YTEntityImpl>field("link").field("c"));
      Assert.assertNotNull(root.<YTEntityImpl>field("link").field("e"));

    } finally {
      database.command("drop class A").close();
      database.command("drop class B").close();
    }
  }

  @Test
  public void testTempRIDsAreNotRecycledInResultSet() {
    final List<YTEntityImpl> resultset =
        executeQuery("select name, $l as l from OUser let $l = (select name from OuSer)");

    Assert.assertNotNull(resultset);

    Set<YTRID> rids = new HashSet<>();
    for (YTIdentifiable d : resultset) {
      final YTRID rid = d.getIdentity();
      Assert.assertFalse(rids.contains(rid));

      rids.add(rid);

      final List<YTIdentifiable> embeddedList = ((YTEntityImpl) d.getRecord()).field("l");
      Assert.assertNotNull(embeddedList);
      Assert.assertFalse(embeddedList.isEmpty());

      for (YTIdentifiable embedded : embeddedList) {
        if (embedded != null) {
          final YTRID embeddedRid = embedded.getIdentity();

          Assert.assertFalse(rids.contains(embeddedRid));
          rids.add(rid);
        }
      }
    }
  }
}
