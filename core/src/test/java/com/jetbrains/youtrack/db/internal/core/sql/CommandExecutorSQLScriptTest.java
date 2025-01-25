package com.jetbrains.youtrack.db.internal.core.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandScript;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class CommandExecutorSQLScriptTest extends DbTestBase {

  public void beforeTest() throws Exception {
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
  public void testQuery() {
    String script = """
        begin
        let $a = select from foo
        commit
        return $a
        """;
    List<EntityImpl> qResult = db.command(new CommandScript("sql", script)).execute(db);

    Assert.assertEquals(3, qResult.size());
  }

  @Test
  public void testTx() {
    String script =
        """
            begin
            let $a = insert into V set test = 'sql script test'
            commit retry 10
            return $a
            """;
    EntityImpl qResult = db.command(new CommandScript("sql", script)).execute(db);

    Assert.assertNotNull(qResult);
  }

  @Test
  public void testReturnExpanded() {
    StringBuilder script = new StringBuilder();
    script.append("begin\n");
    script.append("let $a = insert into V set test = 'sql script test'\n");
    script.append("commit\n");
    script.append("return $a.toJSON()\n");
    String qResult = db.command(new CommandScript("sql", script.toString())).execute(db);
    Assert.assertNotNull(qResult);

    db.newEntity().updateFromJSON(qResult);

    script = new StringBuilder();
    script.append("let $a = select from V limit 2\n");
    script.append("return $a.toJSON()\n");
    String result = db.command(new CommandScript("sql", script.toString())).execute(db);

    Assert.assertNotNull(result);
    result = result.trim();
    Assert.assertTrue(result.startsWith("["));
    Assert.assertTrue(result.endsWith("]"));
    db.newEntity().updateFromJSON(result.substring(1, result.length() - 1));
  }

  @Test
  public void testSleep() {
    long begin = System.currentTimeMillis();

    db.command(new CommandScript("sql", "sleep 500")).execute(db);

    Assert.assertTrue(System.currentTimeMillis() - begin >= 500);
  }

  @Test
  public void testConsoleLog() {
    String script = "LET $a = 'log'\n" + "console.log 'This is a test of log for ${a}'";
    db.command(new CommandScript("sql", script)).execute(db);
  }

  @Test
  public void testConsoleOutput() {
    String script = "LET $a = 'output'\n" + "console.output 'This is a test of log for ${a}'";
    db.command(new CommandScript("sql", script)).execute(db);
  }

  @Test
  public void testConsoleError() {
    String script = "LET $a = 'error'\n" + "console.error 'This is a test of log for ${a}'";
    db.command(new CommandScript("sql", script)).execute(db);
  }

  @Test
  public void testReturnObject() {
    Collection<Object> result =
        db.command(new CommandScript("sql", "return [{ a: 'b' }]")).execute(db);

    Assert.assertNotNull(result);

    Assert.assertEquals(1, result.size());

    Assert.assertTrue(result.iterator().next() instanceof Map);
  }

  @Test
  public void testIncrementAndLet() {

    String script =
        """
            CREATE CLASS TestCounter;
            begin;
            INSERT INTO TestCounter set weight = 3;
            LET counter = SELECT count(*) FROM TestCounter;
            UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;
            commit;
            """;
    List<EntityImpl> qResult = db.command(new CommandScript("sql", script)).execute(db);

    assertThat(db.bindToSession(qResult.get(0)).<Long>field("weight")).isEqualTo(4L);
  }

  @Test
  @Ignore
  public void testIncrementAndLetNewApi() {

    String script =
        """
            CREATE CLASS TestCounter;
            begin;
            INSERT INTO TestCounter set weight = 3;
            LET counter = SELECT count(*) FROM TestCounter;
            UPDATE TestCounter INCREMENT weight = $counter[0].count RETURN AfTER @this;
            commit;
            """;
    ResultSet qResult = db.execute("sql", script);

    assertThat(qResult.next().getEntity().orElseThrow().<Long>getProperty("weight")).isEqualTo(4L);
  }

  @Test
  public void testIf1() {

    String script =
        """
            let $a = select 1 as one;
            if($a[0].one = 1){;
             return 'OK';
            };
            return 'FAIL';
            """;
    var qResult = db.execute("sql", script).stream().findFirst().orElseThrow();

    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testIf2() {

    String script =
        """
            let $a = select 1 as one;
            if    ($a[0].one = 1)   {;
             return 'OK';
                 };     \s
            return 'FAIL';
            """;
    var qResult = db.execute("sql", script).stream().findFirst().orElseThrow();

    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testIf3() {
    var qResult =
        db.execute(

                "sql",
                "let $a = select 1 as one; if($a[0].one = 1){return 'OK';}return 'FAIL';").stream()
            .findFirst().orElseThrow();
    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testNestedIf2() {

    String script =
        """
            let $a = select 1 as one;
            if($a[0].one = 1){;
                if($a[0].one = 'zz'){;
                  return 'FAIL';
                };
              return 'OK';
            };
            return 'FAIL';
            """;
    var qResult = db.execute("sql", script).stream().findFirst().orElseThrow();

    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testNestedIf3() {

    String script =
        """
            let $a = select 1 as one;
            if($a[0].one = 'zz'){;
                if($a[0].one = 1){;
                  return 'FAIL';
                };
              return 'FAIL';
            };
            return 'OK';
            """;
    var qResult = db.execute("sql", script).stream().findFirst().orElseThrow();

    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testIfRealQuery() {

    String script =
        """
            let $a = select from foo
            if($a is not null and $a.size() = 3){
              return $a
            }
            return 'FAIL'
            """;
    Object qResult = db.command(new CommandScript("sql", script)).execute(db);

    Assert.assertNotNull(qResult);
    Assert.assertEquals(3, ((List<?>) qResult).size());
  }

  @Test
  public void testIfMultipleStatements() {

    String script =
        """
            let $a = select 1 as one;
            if($a[0].one = 1){;
              let $b = select 'OK' as ok;
              return $b[0].ok;
            };
            return 'FAIL';
            """;
    var qResult = db.execute("sql", script).stream().findFirst().orElseThrow();

    Assert.assertNotNull(qResult);
    Assert.assertEquals("OK", qResult.getProperty("value"));
  }

  @Test
  public void testScriptSubContext() {

    String script =
        """
            let $a = select from foo limit 1
            select from (traverse doesnotexist from $a)
            """;
    Iterable<?> qResult = db.command(new CommandScript("sql", script)).execute(db);

    Assert.assertNotNull(qResult);
    Iterator<?> iterator = qResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    iterator.next();
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSemicolonInString() {
    // testing parsing problem
    String script =
        """
            let $a = select 'foo ; bar' as one;
            let $b = select 'foo \\'; bar' as one;
            let $a = select "foo ; bar" as one;
            let $b = select "foo \\"; bar" as one;
            """;
    db.execute("sql", script).close();
  }

  @Test
  public void testQuotedRegex() {
    // issue #4996 (simplified)
    db.command("CREATE CLASS QuotedRegex2").close();
    String batch = "begin;INSERT INTO QuotedRegex2 SET regexp=\"'';\";commit;";

    db.command(new CommandScript(batch)).execute(db);

    List<EntityImpl> result = db.query(
        new SQLSynchQuery<EntityImpl>("SELECT FROM QuotedRegex2"));
    Assert.assertEquals(1, result.size());
    EntityImpl doc = result.get(0);
    Assert.assertEquals("'';", doc.field("regexp"));
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

    ResultSet rs = db.execute("sql", script, map);
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

    ResultSet rs = db.execute("sql", script, "bozo", "bozi");
    rs.close();

    rs = db.query("SELECT FROM " + className + " WHERE name = ?", "bozo");

    Assert.assertTrue(rs.hasNext());
    rs.next();
    rs.close();
  }
}
