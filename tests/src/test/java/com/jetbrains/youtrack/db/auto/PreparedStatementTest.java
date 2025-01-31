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

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class PreparedStatementTest extends BaseDBTest {

  @Parameters(value = "remote")
  public PreparedStatementTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    db.command("CREATE CLASS PreparedStatementTest1");
    db.command("insert into PreparedStatementTest1 (name, surname) values ('foo1', 'bar1')");
    db.command(
        "insert into PreparedStatementTest1 (name, listElem) values ('foo2', ['bar2'])");
  }

  @Test
  public void testUnnamedParamTarget() {
    Iterable<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from ?"))
            .execute(db, "PreparedStatementTest1");

    Set<String> expected = new HashSet<String>();
    expected.add("foo1");
    expected.add("foo2");
    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertTrue(expected.contains(doc.field("name")));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTarget() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("className", "PreparedStatementTest1");
    Iterable<EntityImpl> result =
        db.command(new SQLSynchQuery<EntityImpl>("select from :className"))
            .execute(db, params);

    Set<String> expected = new HashSet<String>();
    expected.add("foo1");
    expected.add("foo2");
    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertTrue(expected.contains(doc.field("name")));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTargetRid() {

    Iterable<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(db);

    var record = result.iterator().next();

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("inputRid", record.getIdentity());
    result =
        db.command(new SQLSynchQuery<EntityImpl>("select from :inputRid"))
            .execute(db, params);

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamTargetRid() {

    Iterable<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(db);

    var record = result.iterator().next();
    result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from ?"))
            .execute(db, record.getIdentity());

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTargetDocument() {

    Iterable<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(db);

    var record = result.iterator().next();

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("inputRid", record);
    result =
        db.command(new SQLSynchQuery<EntityImpl>("select from :inputRid"))
            .execute(db, params);

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamTargetDocument() {

    Iterable<EntityImpl> result =
        db
            .command(new SQLSynchQuery<EntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(db);

    var record = result.iterator().next();
    result = db.command(new SQLSynchQuery<EntityImpl>("select from ?"))
        .execute(db, record);

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamFlat() {
    var result = db.query("select from PreparedStatementTest1 where name = ?",
        "foo1");

    var found = false;
    while (result.hasNext()) {
      var doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    var result =
        db.query("select from PreparedStatementTest1 where name = :name", params);

    var found = false;
    while (result.hasNext()) {
      var doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamInArray() {
    Iterable<EntityImpl> result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select from PreparedStatementTest1 where name in [?]"))
            .execute(db, "foo1");

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamInArray() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<EntityImpl> result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select from PreparedStatementTest1 where name in [:name]"))
            .execute(db, params);

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamInArray2() {
    Iterable<EntityImpl> result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select from PreparedStatementTest1 where name in [?, 'antani']"))
            .execute(db, "foo1");

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamInArray2() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<EntityImpl> result =
        db
            .command(
                new SQLSynchQuery<EntityImpl>(
                    "select from PreparedStatementTest1 where name in [:name, 'antani']"))
            .execute(db, params);

    var found = false;
    for (var doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSubqueryUnnamedParamFlat() {
    var result =
        db.query(
            "select from (select from PreparedStatementTest1 where name = ?) where name = ?",
            "foo1",
            "foo1");

    var found = false;
    while (result.hasNext()) {
      var doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSubqueryNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    var result =
        db.query(
            "select from (select from PreparedStatementTest1 where name = :name) where name ="
                + " :name",
            params);

    var found = false;
    while (result.hasNext()) {
      var doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testFunction() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("one", 1);
    params.put("three", 3);
    var result = db.query("select max(:one, :three) as maximo", params);

    var found = false;
    while (result.hasNext()) {
      var doc = result.next();
      found = true;
      Assert.assertEquals(doc.<Object>getProperty("maximo"), 3);
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSqlInjectionOnTarget() {

    try {
      Iterable<EntityImpl> result =
          db
              .command(new SQLSynchQuery<EntityImpl>("select from ?"))
              .execute(db, "PreparedStatementTest1 where name = 'foo'");
      Assert.fail();
    } catch (Exception e) {

    }
  }
}
