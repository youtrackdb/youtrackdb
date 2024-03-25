/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLSelectProjectionsTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public SQLSelectProjectionsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void queryProjectionOk() {
    List<ODocument> result =
        database
            .command(
                "select nick, followings, followers from Profile where nick is defined and"
                    + " followings is defined and followers is defined")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      String[] colNames = d.fieldNames();
      Assert.assertEquals(colNames.length, 3, "document: " + d);
      Assert.assertEquals(colNames[0], "nick", "document: " + d);
      Assert.assertEquals(colNames[1], "followings", "document: " + d);
      Assert.assertEquals(colNames[2], "followers", "document: " + d);

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionObjectLevel() {
    var result =
        database.query("select nick, followings, followers from Profile").stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 3);
      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionLinkedAndFunction() {
    List<ODocument> result =
        database
            .query(
                "select name.toUpperCase(Locale.ENGLISH), address.city.country.name from"
                    + " Profile")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      if (d.field("name") != null)
        Assert.assertEquals(
            ((String) d.field("name")).toUpperCase(Locale.ENGLISH), d.field("name"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSameFieldTwice() {
    List<ODocument> result =
        database
            .query(
                "select name, name.toUpperCase(Locale.ENGLISH) from Profile where name is not"
                    + " null")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertNotNull(d.field("name"));
      Assert.assertNotNull(d.field("name2"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionStaticValues() {
    List<ODocument> result =
        database
            .query(
                "select location.city.country.name, address.city.country.name from Profile"
                    + " where location.city.country.name is not null")
            .stream()
            .map(r -> (ODocument) r.toElement())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {

      Assert.assertNotNull(d.field("location"));
      Assert.assertNull(d.field("address"));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionPrefixAndAppend() {
    List<ODocument> result =
        executeQuery(
            "select *, name.prefix('Mr. ').append(' ').append(surname).append('!') as test"
                + " from Profile where name is not null");

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertEquals(
          d.field("test").toString(), "Mr. " + d.field("name") + " " + d.field("surname") + "!");

      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionFunctionsAndFieldOperators() {
    List<ODocument> result =
        executeQuery(
            "select name.append('.').prefix('Mr. ') as name from Profile where name is not"
                + " null");

    Assert.assertFalse(result.isEmpty());
    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertTrue(d.field("name").toString().startsWith("Mr. "));
      Assert.assertTrue(d.field("name").toString().endsWith("."));

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionSimpleValues() {
    List<ODocument> result = executeQuery("select 10, 'ciao' from Profile LIMIT 1");

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 2);
      Assert.assertEquals(((Integer) d.field("10")).intValue(), 10L);
      Assert.assertEquals(d.field("ciao"), "ciao");

      Assert.assertNull(d.getClassName());
      Assert.assertEquals(ORecordInternal.getRecordType(d), ODocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryProjectionJSON() {
    List<ODocument> result = executeQuery("select @this.toJson() as json from Profile");

    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("json"));

      new ODocument().fromJSON((String) d.field("json"));
    }
  }

  public void queryProjectionRid() {
    List<ODocument> result = executeQuery("select @rid FROM V");
    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("rid"));

      final ORID rid = d.field("rid", ORID.class);
      Assert.assertTrue(rid.isValid());
    }
  }

  public void queryProjectionOrigin() {
    List<ODocument> result = executeQuery("select @raw FROM V");
    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.fieldNames().length <= 1);
      Assert.assertNotNull(d.field("raw"));
    }
  }

  public void queryProjectionEval() {
    List<ODocument> result = executeQuery("select eval('1 + 4') as result");
    Assert.assertEquals(result.size(), 1);

    for (ODocument d : result) Assert.assertEquals(d.<Object>field("result"), 5);
  }

  public void queryProjectionContextArray() {
    List<ODocument> result =
        executeQuery("select $a[0] as a0, $a as a from V let $a = outE() where outE().size() > 0");
    Assert.assertFalse(result.isEmpty());

    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("a"));
      Assert.assertTrue(d.containsField("a0"));

      final ODocument a0doc = d.field("a0");
      final ODocument firstADoc =
          (ODocument) d.<Iterable<OIdentifiable>>field("a").iterator().next();

      Assert.assertTrue(
          ODocumentHelper.hasSameContentOf(a0doc, database, firstADoc, database, null));
    }
  }

  public void ifNullFunction() {
    List<ODocument> result = executeQuery("SELECT ifnull('a', 'b')");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "a");

    result = executeQuery("SELECT ifnull('a', 'b', 'c')");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "c");

    result = executeQuery("SELECT ifnull(null, 'b')");
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.get(0).field("ifnull"), "b");
  }

  public void filteringArrayInChain() {
    List<ODocument> result = executeQuery("SELECT set(name)[0-1] as set from OUser");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(OMultiValue.isMultiValue(d.<Object>field("set")));
      Assert.assertTrue(OMultiValue.getSize(d.field("set")) <= 2);
    }

    result = executeQuery("SELECT set(name)[0,1] as set from OUser");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(OMultiValue.isMultiValue(d.<Object>field("set")));
      Assert.assertTrue(OMultiValue.getSize(d.field("set")) <= 2);
    }

    result = executeQuery("SELECT set(name)[0] as unique from OUser");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertFalse(OMultiValue.isMultiValue(d.<Object>field("unique")));
    }
  }

  public void projectionWithNoTarget() {
    List<ODocument> result = executeQuery("select 'Ay' as a , 'bEE'");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("bEE"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' as b fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("b"), "bEE");
    }

    result = executeQuery("select 'Ay' as a , 'bEE' fetchplan *:1");
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertEquals(d.field("a"), "Ay");
      Assert.assertEquals(d.field("bEE"), "bEE");
    }
  }

  @Test()
  public void testSelectExcludeFunction() {
    try {
      database.command("create class A extends V").close();
      database.command("create class B extends E").close();
      OIdentifiable id =
          database.command("insert into A (a,b) values ('a','b')").next().getRecordId();
      Assert.assertNotNull(id);
      OIdentifiable id2 =
          database.command("insert into A (a,b) values ('a','b')").next().getRecordId();
      Assert.assertNotNull(id2);
      OIdentifiable id3 =
          database.command("insert into A (a,b) values ('a','b')").next().getRecordId();
      Assert.assertNotNull(id3);
      OIdentifiable id4 =
          database.command("insert into A (a,b) values ('a','b')").next().getRecordId();
      Assert.assertNotNull(id4);
      database
          .command("create edge B from " + id.getIdentity() + " to " + id2.getIdentity())
          .close();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id3.getIdentity())
          .close();
      database
          .command("create edge B from " + id.getIdentity() + " to " + id4.getIdentity())
          .close();
      database
          .command("create edge B from " + id4.getIdentity() + " to " + id.getIdentity())
          .close();

      List<ODocument> res =
          executeQuery(
              "select a,b,in_B.out.exclude('out_B') from "
                  + id2.getIdentity()
                  + " fetchplan in_B.out:1");

      Assert.assertNotNull(res.get(0).field("a"));
      Assert.assertNotNull(res.get(0).field("b"));
      //noinspection unchecked
      Assert.assertNull((((List<ODocument>) res.get(0).field("in_B")).get(0).field("out_B")));

      res =
          executeQuery(
              "SELECT out.exclude('in_B') FROM ( SELECT EXPAND(in_B) FROM "
                  + id2.getIdentity()
                  + " ) FETCHPLAN out:0 ");

      Assert.assertNotNull(res.get(0).field("out"));
      Assert.assertNotNull(((ODocument) res.get(0).field("out")).field("a"));
      Assert.assertNull(((ODocument) res.get(0).field("out")).field("in_B"));
    } finally {
      database.command("drop class A unsafe ").close();
      database.command("drop class B unsafe ").close();
    }
  }

  @Test
  public void testSimpleExpandExclude() {
    try {
      database.command("create class A extends V").close();
      database.command("create class B extends E").close();
      database.command("create class C extends E").close();
      OIdentifiable id =
          database.command("insert into A (a,b) values ('a1','b1')").next().getRecordId();
      Assert.assertNotNull(id);
      OIdentifiable id2 =
          database.command("insert into A (a,b) values ('a2','b2')").next().getRecordId();
      Assert.assertNotNull(id2);
      OIdentifiable id3 =
          database.command("insert into A (a,b) values ('a3','b3')").next().getRecordId();
      Assert.assertNotNull(id3);
      database
          .command("create edge B from " + id.getIdentity() + " to " + id2.getIdentity())
          .close();
      database
          .command("create edge C from " + id2.getIdentity() + " to " + id3.getIdentity())
          .close();

      List<ODocument> res =
          executeQuery(
              "select out.exclude('in_B') from (select expand(in_C) from "
                  + id3.getIdentity()
                  + " )");
      Assert.assertEquals(res.size(), 1);
      ODocument ele = res.get(0);
      Assert.assertNotNull(ele.field("out"));
      Assert.assertEquals(((ODocument) ele.field("out")).field("a"), "a2");
      Assert.assertNull(((ODocument) ele.field("out")).field("in_B"));

    } finally {
      database.command("drop class A unsafe ").close();
      database.command("drop class B unsafe ").close();
      database.command("drop class C unsafe ").close();
    }
  }

  @Test
  public void testTempRIDsAreNotRecycledInResultSet() {
    final List<ODocument> resultset =
        executeQuery("select name, $l as l from OUser let $l = (select name from OuSer)");

    Assert.assertNotNull(resultset);

    Set<ORID> rids = new HashSet<>();
    for (OIdentifiable d : resultset) {
      final ORID rid = d.getIdentity();
      Assert.assertFalse(rids.contains(rid));

      rids.add(rid);

      final List<OIdentifiable> embeddedList = ((ODocument) d.getRecord()).field("l");
      Assert.assertNotNull(embeddedList);
      Assert.assertFalse(embeddedList.isEmpty());

      for (OIdentifiable embedded : embeddedList) {
        if (embedded != null) {
          final ORID embeddedRid = embedded.getIdentity();

          Assert.assertFalse(rids.contains(embeddedRid));
          rids.add(rid);
        }
      }
    }
  }
}
