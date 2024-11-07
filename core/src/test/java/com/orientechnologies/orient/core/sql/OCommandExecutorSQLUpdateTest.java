/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

public class OCommandExecutorSQLUpdateTest extends BaseMemoryDatabase {

  @Test
  public void testUpdateRemoveAll() throws Exception {

    db.command("CREATE class company").close();
    db.command("CREATE property company.name STRING").close();
    db.command("CREATE class employee").close();
    db.command("CREATE property employee.name STRING").close();
    db.command("CREATE property company.employees LINKSET employee").close();

    db.begin();
    db.command("INSERT INTO company SET name = 'MyCompany'").close();
    db.commit();

    final OElement r = db.query("SELECT FROM company").next().getElement().get();

    db.begin();
    db.command("INSERT INTO employee SET name = 'Philipp'").close();
    db.command("INSERT INTO employee SET name = 'Selma'").close();
    db.command("INSERT INTO employee SET name = 'Thierry'").close();
    db.command("INSERT INTO employee SET name = 'Linn'").close();

    db.command("UPDATE company set employees = (SELECT FROM employee)").close();
    db.commit();

    assertEquals(((Set) db.bindToSession(r).getProperty("employees")).size(), 4);

    db.begin();
    db.command(
            "UPDATE company REMOVE employees = (SELECT FROM employee WHERE name = 'Linn') WHERE"
                + " name = 'MyCompany'")
        .close();
    db.commit();

    assertEquals(((Set) db.bindToSession(r).getProperty("employees")).size(), 3);
  }

  @Test
  public void testUpdateContent() throws Exception {
    db.begin();
    db.command("insert into V (name) values ('bar')").close();
    db.command("UPDATE V content {\"value\":\"foo\"}").close();
    db.commit();

    try (OResultSet result = db.query("select from V")) {
      OResult doc = result.next();
      assertEquals(doc.getProperty("value"), "foo");
    }
  }

  @Test
  public void testUpdateContentParse() throws Exception {
    db.begin();
    db.command("insert into V (name) values ('bar')").close();
    db.command("UPDATE V content {\"value\":\"foo\\\\\"}").close();
    db.commit();

    try (OResultSet result = db.query("select from V")) {
      assertEquals(result.next().getProperty("value"), "foo\\");
    }

    db.begin();
    db.command("UPDATE V content {\"value\":\"foo\\\\\\\\\"}").close();

    try (OResultSet result = db.query("select from V")) {
      assertEquals(result.next().getProperty("value"), "foo\\\\");
    }
    db.commit();
  }

  @Test
  public void testUpdateMergeWithIndex() {
    db.command("CREATE CLASS i_have_a_list ").close();
    db.command("CREATE PROPERTY i_have_a_list.id STRING").close();
    db.command("CREATE INDEX i_have_a_list.id ON i_have_a_list (id) UNIQUE").close();
    db.command("CREATE PROPERTY i_have_a_list.types EMBEDDEDLIST STRING").close();
    db.command("CREATE INDEX i_have_a_list.types ON i_have_a_list (types) NOTUNIQUE").close();

    db.begin();
    db.command(
            "INSERT INTO i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"aaa\", \"bbb\"]}")
        .close();

    OResultSet result = db.query("SELECT * FROM i_have_a_list WHERE types = 'aaa'");
    assertEquals(result.stream().count(), 1);

    db.command(
            "UPDATE i_have_a_list CONTENT {\"id\": \"the_id\", \"types\": [\"ccc\", \"bbb\"]} WHERE"
                + " id = 'the_id'")
        .close();
    db.commit();

    result = db.query("SELECT * FROM i_have_a_list WHERE types = 'ccc'");
    assertEquals(result.stream().count(), 1);

    result = db.query("SELECT * FROM i_have_a_list WHERE types = 'aaa'");
    assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testNamedParamsSyntax() {
    // issue #4470
    String className = getClass().getSimpleName() + "_NamedParamsSyntax";

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "foo");
    params.put("full_name", "foo");
    params.put("html_url", "foo");
    params.put("description", "foo");
    params.put("git_url", "foo");
    params.put("ssh_url", "foo");
    params.put("clone_url", "foo");
    params.put("svn_url", "foo");
    db.command("create class " + className).close();

    db.command(
            "update "
                + className
                + " SET name = :name, full_name = :full_name, html_url = :html_url, description ="
                + " :description, git_url = :git_url, ssh_url = :ssh_url, clone_url = :clone_url,"
                + " svn_url = :svn_urlUPSERT WHERE full_name = :full_name",
            params)
        .close();

    db.command(
            "update "
                + className
                + " SET name = :name, html_url = :html_url, description = :description, git_url ="
                + " :git_url, ssh_url = :ssh_url, clone_url = :clone_url, svn_url = :svn_urlUPSERT"
                + " WHERE full_name = :full_name",
            params)
        .close();
  }

  @Test
  public void testUpsertSetPut() throws Exception {
    db.command("CREATE CLASS test").close();
    db.command("CREATE PROPERTY test.id integer").close();
    db.command("CREATE PROPERTY test.addField EMBEDDEDSET string").close();

    db.begin();
    db.command("UPDATE test SET id = 1 , addField=[\"xxxx\"] UPSERT WHERE id = 1").close();
    db.commit();

    try (OResultSet result = db.query("select from test")) {
      OResult doc = result.next();
      Set<?> set = doc.getProperty("addField");
      assertEquals(set.size(), 1);
      assertEquals(set.iterator().next(), "xxxx");
    }
  }

  @Test
  public void testUpdateParamDate() throws Exception {
    db.command("CREATE CLASS test").close();
    Date date = new Date();

    db.begin();
    db.command("insert into test set birthDate = ?", date).close();
    db.commit();
    try (OResultSet result = db.query("select from test")) {
      OResult doc = result.next();
      assertEquals(doc.getProperty("birthDate"), date);
    }

    date = new Date();
    db.begin();
    db.command("UPDATE test set birthDate = ?", date).close();
    db.commit();

    try (OResultSet result = db.query("select from test")) {
      OResult doc = result.next();
      assertEquals(doc.getProperty("birthDate"), date);
    }
  }

  // issue #4776
  @Test
  public void testBooleanListNamedParameter() {
    db.getMetadata().getSchema().createClass("test");

    db.begin();
    ODocument doc = new ODocument("test");
    doc.field("id", 1);
    doc.field("boolean", false);
    doc.field("integerList", Collections.EMPTY_LIST);
    doc.field("booleanList", Collections.EMPTY_LIST);
    db.save(doc);
    db.commit();

    Map<String, Object> params = new HashMap<String, Object>();

    params.put("boolean", true);

    List<Object> integerList = new ArrayList<Object>();
    integerList.add(1);
    params.put("integerList", integerList);

    List<Object> booleanList = new ArrayList<Object>();
    booleanList.add(true);
    params.put("booleanList", booleanList);

    db.begin();
    db.command(
            "UPDATE test SET boolean = :boolean, booleanList = :booleanList, integerList ="
                + " :integerList WHERE id = 1",
            params)
        .close();
    db.commit();

    try (OResultSet queryResult = db.command("SELECT * FROM test WHERE id = 1")) {
      OResult docResult = queryResult.next();
      List<?> resultBooleanList = docResult.getProperty("booleanList");
      assertNotNull(resultBooleanList);
      assertEquals(resultBooleanList.size(), 1);
      assertEquals(resultBooleanList.iterator().next(), true);
      assertFalse(queryResult.hasNext());
    }
  }

  @Test
  public void testIncrementWithDotNotationField() throws Exception {

    db.command("CREATE class test").close();

    db.begin();
    final ODocument test = new ODocument("test");
    test.field("id", "id1");
    test.field("count", 20);

    Map<String, Integer> nestedCound = new HashMap<String, Integer>();
    nestedCound.put("nestedCount", 10);
    test.field("map", nestedCound);

    db.save(test);
    db.commit();

    OElement queried = db.query("SELECT FROM test WHERE id = \"id1\"").next().getElement().get();

    db.begin();
    db.command("UPDATE test set count += 2").close();
    db.commit();

    Assertions.assertThat(db.bindToSession(queried).<Integer>getProperty("count")).isEqualTo(22);

    db.begin();
    db.command("UPDATE test set map.nestedCount = map.nestedCount + 5").close();
    db.commit();

    Assertions.assertThat(db.bindToSession(queried).<Map>getProperty("map").get("nestedCount"))
        .isEqualTo(15);

    db.begin();
    db.command("UPDATE test set map.nestedCount = map.nestedCount+ 5").close();
    db.commit();

    Assertions.assertThat(db.bindToSession(queried).<Map>getProperty("map").get("nestedCount"))
        .isEqualTo(20);
  }

  @Test
  public void testSingleQuoteInNamedParameter() throws Exception {
    db.command("CREATE class test").close();

    db.begin();
    final ODocument test = new ODocument("test");
    test.field("text", "initial value");
    db.save(test);
    db.commit();

    OElement queried = db.query("SELECT FROM test").next().getElement().get();
    assertEquals(queried.getProperty("text"), "initial value");

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("text", "single \"");

    db.begin();
    db.command("UPDATE test SET text = :text", params).close();
    db.commit();

    assertEquals(db.bindToSession(queried).getProperty("text"), "single \"");
  }

  @Test
  public void testQuotedStringInNamedParameter() throws Exception {

    db.command("CREATE class test").close();

    db.begin();
    final ODocument test = new ODocument("test");
    test.field("text", "initial value");

    db.save(test);
    db.commit();

    OElement queried = db.query("SELECT FROM test").next().getElement().get();
    assertEquals(queried.getProperty("text"), "initial value");

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("text", "quoted \"value\" string");

    db.begin();
    db.command("UPDATE test SET text = :text", params).close();
    db.commit();

    assertEquals(db.bindToSession(queried).getProperty("text"), "quoted \"value\" string");
  }

  @Test
  public void testQuotesInJson() throws Exception {

    db.command("CREATE class testquotesinjson").close();

    db.begin();
    db.command(
            "UPDATE testquotesinjson SET value = {\"f12\":'test\\\\'} UPSERT WHERE key = \"test\"")
        .close();
    db.commit();

    OElement queried = db.query("SELECT FROM testquotesinjson").next().getElement().get();
    assertEquals(((Map) queried.getProperty("value")).get("f12"), "test\\");
  }

  @Test
  public void testDottedTargetInScript() {
    // #issue #5397
    db.command("create class A").close();
    db.command("create class B").close();

    db.begin();
    db.command("insert into A set name = 'foo'").close();
    db.command("insert into B set name = 'bar', a = (select from A)").close();
    db.commit();

    StringBuilder script = new StringBuilder();
    script.append("let $a = select from B;\n");
    script.append("update $a.a set name = 'baz';\n");

    db.begin();
    db.command(new OCommandScript(script.toString())).execute();
    db.commit();

    try (OResultSet result = db.query("select from A")) {
      assertEquals(result.next().getProperty("name"), "baz");
      assertFalse(result.hasNext());
    }
  }

  @Test
  public void testBacktickClassName() throws Exception {
    db.getMetadata().getSchema().createClass("foo-bar");
    db.begin();
    db.command("insert into `foo-bar` set name = 'foo'").close();
    db.command("UPDATE `foo-bar` set name = 'bar' where name = 'foo'").close();
    db.commit();

    try (OResultSet result = db.query("select from `foo-bar`")) {
      assertEquals(result.next().getProperty("name"), "bar");
    }
  }

  @Test
  @Ignore
  public void testUpdateLockLimit() throws Exception {
    db.getMetadata().getSchema().createClass("foo");
    db.command("insert into foo set name = 'foo'").close();
    db.command("UPDATE foo set name = 'bar' where name = 'foo' lock record limit 1").close();
    try (OResultSet result = db.query("select from foo")) {
      assertEquals(result.next().getProperty("name"), "bar");
    }
    db.command("UPDATE foo set name = 'foo' where name = 'bar' lock record limit 1").close();
  }

  @Test
  public void testUpdateContentOnClusterTarget() throws Exception {
    db.command("CREATE class Foo").close();
    db.command("CREATE class Bar").close();
    db.command("CREATE property Foo.bar EMBEDDED Bar").close();

    db.begin();
    db.command("insert into cluster:foo set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("UPDATE cluster:foo set bar = {\"value\":\"foo\\\\\"}").close();
    db.commit();

    try (OResultSet result = db.query("select from cluster:foo")) {
      assertEquals(((OResult) result.next().getProperty("bar")).getProperty("value"), "foo\\");
    }
  }

  @Test
  public void testUpdateContentOnClusterTargetMultiple() throws Exception {
    db.command("CREATE class Foo").close();
    db.command("ALTER CLASS Foo addcluster fooadditional1").close();
    db.command("ALTER CLASS Foo addcluster fooadditional2").close();
    db.command("CREATE class Bar").close();
    db.command("CREATE property Foo.bar EMBEDDED Bar").close();

    db.begin();
    db.command("insert into cluster:foo set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("UPDATE cluster:foo set bar = {\"value\":\"foo\\\\\"}").close();
    db.commit();

    try (OResultSet result = db.query("select from cluster:foo")) {
      assertTrue(result.hasNext());
      OResult doc = result.next();
      assertEquals(((OResult) doc.getProperty("bar")).getProperty("value"), "foo\\");
      assertFalse(result.hasNext());
    }

    db.begin();
    db.command("insert into cluster:fooadditional1 set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("UPDATE cluster:fooadditional1 set bar = {\"value\":\"foo\\\\\"}").close();
    db.commit();

    try (OResultSet result = db.query("select from cluster:fooadditional1")) {
      assertTrue(result.hasNext());
      OResult doc = result.next();
      assertEquals(((OResult) doc.getProperty("bar")).getProperty("value"), "foo\\");
      assertFalse(result.hasNext());
    }
  }

  @Test
  public void testUpdateContentOnClusterTargetMultipleSelection() throws Exception {
    db.command("CREATE class Foo").close();
    db.command("ALTER CLASS Foo addcluster fooadditional1").close();
    db.command("ALTER CLASS Foo addcluster fooadditional2").close();
    db.command("ALTER CLASS Foo addcluster fooadditional3").close();
    db.command("CREATE class Bar").close();
    db.command("CREATE property Foo.bar EMBEDDED Bar").close();

    db.begin();
    db.command("insert into cluster:fooadditional1 set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("insert into cluster:fooadditional2 set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("insert into cluster:fooadditional3 set bar = {\"value\":\"zz\\\\\"}").close();
    db.command("UPDATE cluster:[fooadditional1, fooadditional2] set bar = {\"value\":\"foo\\\\\"}")
        .close();
    db.commit();

    OResultSet resultSet = db.query("select from cluster:[ fooadditional1, fooadditional2 ]");
    assertTrue(resultSet.hasNext());
    OResult doc = resultSet.next();
    assertEquals(((OResult) doc.getProperty("bar")).getProperty("value"), "foo\\");
    assertTrue(resultSet.hasNext());
    doc = resultSet.next();
    assertEquals(((OResult) doc.getProperty("bar")).getProperty("value"), "foo\\");
    assertFalse(resultSet.hasNext());
    resultSet.close();
  }

  @Test
  public void testUpdateContentNotORestricted() throws Exception {
    // issue #5564
    db.command("CREATE class Foo").close();

    db.begin();
    ODocument d = new ODocument("Foo");
    d.field("name", "foo");
    d.save();

    db.command("update Foo MERGE {\"a\":1}").close();
    db.command("update Foo CONTENT {\"a\":1}").close();
    db.commit();

    try (OResultSet result = db.query("select from Foo")) {

      OResult doc = result.next();
      assertNull(doc.getProperty("_allowRead"));
      assertFalse(result.hasNext());
    }
  }

  @Test
  public void testUpdateReturnCount() throws Exception {
    // issue #5564
    db.command("CREATE class Foo").close();

    db.begin();
    ODocument d = new ODocument("Foo");
    d.field("name", "foo");
    d.save();
    db.commit();

    db.begin();
    d = new ODocument("Foo");
    d.field("name", "bar");
    d.save();
    db.commit();

    db.begin();
    OResultSet result = db.command("update Foo set surname = 'baz' return count");
    db.commit();

    assertEquals(2, (long) result.next().getProperty("count"));
  }

  @Test
  public void testLinkedUpdate() {
    db.command("CREATE class TestSource").close();
    db.command("CREATE class TestLinked").close();
    db.command("CREATE property TestLinked.id STRING").close();
    db.command("CREATE INDEX TestLinked.id ON TestLinked (id) UNIQUE_HASH_INDEX ENGINE HASH_INDEX")
        .close();

    db.begin();
    ODocument state = new ODocument("TestLinked");
    state.setProperty("id", "idvalue");
    db.save(state);
    db.commit();

    db.begin();
    ODocument d = new ODocument("TestSource");
    state = db.bindToSession(state);
    d.setProperty("name", "foo");
    d.setProperty("linked", state);
    db.save(d);
    db.commit();

    db.begin();
    db.command(
            "Update TestSource set flag = true , linked.flag = true return after *, linked:{*} as"
                + " infoLinked  where name = \"foo\"")
        .close();
    db.commit();

    OResultSet result = db.query("select from TestLinked where id = \"idvalue\"");
    while (result.hasNext()) {
      OResult res = result.next();
      assertTrue(res.hasProperty("flag"));
      assertTrue(res.getProperty("flag"));
    }
  }
}
