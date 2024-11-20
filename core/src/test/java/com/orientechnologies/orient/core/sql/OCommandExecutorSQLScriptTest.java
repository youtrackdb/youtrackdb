package com.orientechnologies.orient.core.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OCommandExecutorSQLScriptTest extends BaseMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();

    db.command("CREATE class foo").close();

    db.begin();
    db.command("insert into foo (name, bar) values ('a', 1)").close();
    db.command("insert into foo (name, bar) values ('b', 2)").close();
    db.command("insert into foo (name, bar) values ('c', 3)").close();
    db.commit();

    db.activateOnCurrentThread();
  }

  @Test
  public void testQuery() throws Exception {
    String script = "begin\n" + "let $a = select from foo\n" + "commit\n" + "return $a\n";
    List<ODocument> qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertEquals(qResult.size(), 3);
  }

  @Test
  public void testTx() throws Exception {
    String script =
        "begin\n"
            + "let $a = insert into V set test = 'sql script test'\n"
            + "commit retry 10\n"
            + "return $a\n";
    ODocument qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
  }

  @Test
  public void testReturnExpanded() throws Exception {
    StringBuilder script = new StringBuilder();
    script.append("begin\n");
    script.append("let $a = insert into V set test = 'sql script test'\n");
    script.append("commit\n");
    script.append("return $a.toJSON()\n");
    String qResult = db.command(new OCommandScript("sql", script.toString())).execute();
    Assert.assertNotNull(qResult);

    new ODocument().fromJSON(qResult);

    script = new StringBuilder();
    script.append("let $a = select from V limit 2\n");
    script.append("return $a.toJSON()\n");
    String result = db.command(new OCommandScript("sql", script.toString())).execute();

    Assert.assertNotNull(result);
    result = result.trim();
    Assert.assertTrue(result.startsWith("["));
    Assert.assertTrue(result.endsWith("]"));
    new ODocument().fromJSON(result.substring(1, result.length() - 1));
  }

  @Test
  public void testSleep() throws Exception {
    long begin = System.currentTimeMillis();

    db.command(new OCommandScript("sql", "sleep 500")).execute();

    Assert.assertTrue(System.currentTimeMillis() - begin >= 500);
  }

  @Test
  public void testConsoleLog() throws Exception {
    String script = "LET $a = 'log'\n" + "console.log 'This is a test of log for ${a}'";
    db.command(new OCommandScript("sql", script)).execute();
  }

  @Test
  public void testConsoleOutput() throws Exception {
    String script = "LET $a = 'output'\n" + "console.output 'This is a test of log for ${a}'";
    db.command(new OCommandScript("sql", script)).execute();
  }

  @Test
  public void testConsoleError() throws Exception {
    String script = "LET $a = 'error'\n" + "console.error 'This is a test of log for ${a}'";
    db.command(new OCommandScript("sql", script)).execute();
  }

  @Test
  public void testReturnObject() throws Exception {
    Collection<Object> result =
        db.command(new OCommandScript("sql", "return [{ a: 'b' }]")).execute();

    Assert.assertNotNull(result);

    Assert.assertEquals(result.size(), 1);

    Assert.assertTrue(result.iterator().next() instanceof Map);
  }

  @Test
  public void testIncrementAndLet() throws Exception {

    String script =
        "CREATE CLASS TestCounter;\n"
            + "begin;\n"
            + "INSERT INTO TestCounter set weight = 3;\n"
            + "LET counter = SELECT count(*) FROM TestCounter;\n"
            + "UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n"
            + "commit;\n";
    List<ODocument> qResult = db.command(new OCommandScript("sql", script)).execute();

    assertThat(db.bindToSession(qResult.get(0)).<Long>field("weight")).isEqualTo(4L);
  }

  @Test
  @Ignore
  public void testIncrementAndLetNewApi() throws Exception {

    String script =
        "CREATE CLASS TestCounter;\n"
            + "begin;\n"
            + "INSERT INTO TestCounter set weight = 3;\n"
            + "LET counter = SELECT count(*) FROM TestCounter;\n"
            + "UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;\n"
            + "commit;\n";
    OResultSet qResult = db.execute("sql", script);

    assertThat(qResult.next().getElement().get().<Long>getProperty("weight")).isEqualTo(4L);
  }

  @Test
  public void testIf1() throws Exception {

    String script =
        "let $a = select 1 as one\n"
            + "if($a[0].one = 1){\n"
            + " return 'OK'\n"
            + "}\n"
            + "return 'FAIL'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIf2() throws Exception {

    String script =
        "let $a = select 1 as one\n"
            + "if    ($a[0].one = 1)   { \n"
            + " return 'OK'\n"
            + "     }      \n"
            + "return 'FAIL'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIf3() throws Exception {
    Object qResult =
        db.command(
                new OCommandScript(
                    "sql",
                    "let $a = select 1 as one; if($a[0].one = 1){return 'OK';}return 'FAIL';"))
            .execute();
    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testNestedIf2() throws Exception {

    String script =
        "let $a = select 1 as one\n"
            + "if($a[0].one = 1){\n"
            + "    if($a[0].one = 'zz'){\n"
            + "      return 'FAIL'\n"
            + "    }\n"
            + "  return 'OK'\n"
            + "}\n"
            + "return 'FAIL'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testNestedIf3() throws Exception {

    String script =
        "let $a = select 1 as one\n"
            + "if($a[0].one = 'zz'){\n"
            + "    if($a[0].one = 1){\n"
            + "      return 'FAIL'\n"
            + "    }\n"
            + "  return 'FAIL'\n"
            + "}\n"
            + "return 'OK'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testIfRealQuery() throws Exception {

    String script =
        "let $a = select from foo\n"
            + "if($a is not null and $a.size() = 3){\n"
            + "  return $a\n"
            + "}\n"
            + "return 'FAIL'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(((List) qResult).size(), 3);
  }

  @Test
  public void testIfMultipleStatements() throws Exception {

    String script =
        "let $a = select 1 as one\n"
            + "if($a[0].one = 1){\n"
            + "  let $b = select 'OK' as ok\n"
            + "  return $b[0].ok\n"
            + "}\n"
            + "return 'FAIL'\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Assert.assertEquals(qResult, "OK");
  }

  @Test
  public void testScriptSubContext() throws Exception {

    String script =
        "let $a = select from foo limit 1\n" + "select from (traverse doesnotexist from $a)\n";
    Iterable qResult = db.command(new OCommandScript("sql", script)).execute();

    Assert.assertNotNull(qResult);
    Iterator iterator = qResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    iterator.next();
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSemicolonInString() throws Exception {
    // issue https://github.com/orientechnologies/orientjs/issues/133
    // testing parsing problem

    String script =
        "let $a = select 'foo ; bar' as one\n"
            + "let $b = select 'foo \\'; bar' as one\n"
            + "let $a = select \"foo ; bar\" as one\n"
            + "let $b = select \"foo \\\"; bar\" as one\n";
    Object qResult = db.command(new OCommandScript("sql", script)).execute();
  }

  @Test
  public void testQuotedRegex() {
    // issue #4996 (simplified)
    db.command("CREATE CLASS QuotedRegex2").close();
    String batch = "begin;INSERT INTO QuotedRegex2 SET regexp=\"'';\";commit;";

    db.command(new OCommandScript(batch)).execute();

    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("SELECT FROM QuotedRegex2"));
    Assert.assertEquals(result.size(), 1);
    ODocument doc = result.get(0);
    Assert.assertEquals(doc.field("regexp"), "'';");
  }

  @Test
  public void testParameters1() {
    String className = "testParameters1";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = :name;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = :_name2;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "bozo");
    map.put("_name2", "bozi");

    OResultSet rs = db.execute("sql", script, map);
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }

  @Test
  public void testPositionalParameters() {
    String className = "testPositionalParameters";
    db.createVertexClass(className);
    String script =
        "BEGIN;"
            + "LET $a = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $b = CREATE VERTEX "
            + className
            + " SET name = ?;"
            + "LET $edge = CREATE EDGE E from $a to $b;"
            + "COMMIT;"
            + "RETURN $edge;";

    OResultSet rs = db.execute("sql", script, "bozo", "bozi");
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }
}
