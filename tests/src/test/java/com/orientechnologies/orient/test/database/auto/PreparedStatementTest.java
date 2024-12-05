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

import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class PreparedStatementTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public PreparedStatementTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    database.command("CREATE CLASS PreparedStatementTest1");
    database.command("insert into PreparedStatementTest1 (name, surname) values ('foo1', 'bar1')");
    database.command(
        "insert into PreparedStatementTest1 (name, listElem) values ('foo2', ['bar2'])");
  }

  @Test
  public void testUnnamedParamTarget() {
    Iterable<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from ?"))
            .execute(database, "PreparedStatementTest1");

    Set<String> expected = new HashSet<String>();
    expected.add("foo1");
    expected.add("foo2");
    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertTrue(expected.contains(doc.field("name")));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTarget() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("className", "PreparedStatementTest1");
    Iterable<YTEntityImpl> result =
        database.command(new OSQLSynchQuery<YTEntityImpl>("select from :className"))
            .execute(database, params);

    Set<String> expected = new HashSet<String>();
    expected.add("foo1");
    expected.add("foo2");
    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertTrue(expected.contains(doc.field("name")));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTargetRid() {

    Iterable<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(database);

    YTEntityImpl record = result.iterator().next();

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("inputRid", record.getIdentity());
    result =
        database.command(new OSQLSynchQuery<YTEntityImpl>("select from :inputRid"))
            .execute(database, params);

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamTargetRid() {

    Iterable<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(database);

    YTEntityImpl record = result.iterator().next();
    result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from ?"))
            .execute(database, record.getIdentity());

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamTargetDocument() {

    Iterable<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(database);

    YTEntityImpl record = result.iterator().next();

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("inputRid", record);
    result =
        database.command(new OSQLSynchQuery<YTEntityImpl>("select from :inputRid"))
            .execute(database, params);

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamTargetDocument() {

    Iterable<YTEntityImpl> result =
        database
            .command(new OSQLSynchQuery<YTEntityImpl>("select from PreparedStatementTest1 limit 1"))
            .execute(database);

    YTEntityImpl record = result.iterator().next();
    result = database.command(new OSQLSynchQuery<YTEntityImpl>("select from ?"))
        .execute(database, record);

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.getIdentity(), record.getIdentity());
      Assert.assertEquals(doc.<Object>field("name"), record.field("name"));
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamFlat() {
    YTResultSet result = database.query("select from PreparedStatementTest1 where name = ?",
        "foo1");

    boolean found = false;
    while (result.hasNext()) {
      YTResult doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    YTResultSet result =
        database.query("select from PreparedStatementTest1 where name = :name", params);

    boolean found = false;
    while (result.hasNext()) {
      YTResult doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamInArray() {
    Iterable<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from PreparedStatementTest1 where name in [?]"))
            .execute(database, "foo1");

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamInArray() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from PreparedStatementTest1 where name in [:name]"))
            .execute(database, params);

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testUnnamedParamInArray2() {
    Iterable<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from PreparedStatementTest1 where name in [?, 'antani']"))
            .execute(database, "foo1");

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testNamedParamInArray2() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    Iterable<YTEntityImpl> result =
        database
            .command(
                new OSQLSynchQuery<YTEntityImpl>(
                    "select from PreparedStatementTest1 where name in [:name, 'antani']"))
            .execute(database, params);

    boolean found = false;
    for (YTEntityImpl doc : result) {
      found = true;
      Assert.assertEquals(doc.field("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSubqueryUnnamedParamFlat() {
    YTResultSet result =
        database.query(
            "select from (select from PreparedStatementTest1 where name = ?) where name = ?",
            "foo1",
            "foo1");

    boolean found = false;
    while (result.hasNext()) {
      YTResult doc = result.next();
      found = true;
      Assert.assertEquals(doc.getProperty("name"), "foo1");
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSubqueryNamedParamFlat() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo1");
    YTResultSet result =
        database.query(
            "select from (select from PreparedStatementTest1 where name = :name) where name ="
                + " :name",
            params);

    boolean found = false;
    while (result.hasNext()) {
      YTResult doc = result.next();
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
    YTResultSet result = database.query("select max(:one, :three) as maximo", params);

    boolean found = false;
    while (result.hasNext()) {
      YTResult doc = result.next();
      found = true;
      Assert.assertEquals(doc.<Object>getProperty("maximo"), 3);
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSqlInjectionOnTarget() {

    try {
      Iterable<YTEntityImpl> result =
          database
              .command(new OSQLSynchQuery<YTEntityImpl>("select from ?"))
              .execute(database, "PreparedStatementTest1 where name = 'foo'");
      Assert.fail();
    } catch (Exception e) {

    }
  }
}
