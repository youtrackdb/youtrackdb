package com.orientechnologies.orient.core.command;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OSqlScriptExecutorTest extends DBTestBase {

  @Test
  public void testPlain() {
    String script = "begin;\n";
    script += "insert into V set name ='a';\n";
    script += "insert into V set name ='b';\n";
    script += "insert into V set name ='c';\n";
    script += "insert into V set name ='d';\n";
    script += "commit;\n";
    script += "select from v;";

    YTResultSet result = db.execute("sql", script);
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));
    Assert.assertTrue(list.contains("b"));
    Assert.assertTrue(list.contains("c"));
    Assert.assertTrue(list.contains("d"));
    Assert.assertEquals(4, list.size());
  }

  @Test
  public void testWithPositionalParams() {
    String script = "begin;\n";
    script += "insert into V set name ='a';\n";
    script += "insert into V set name ='b';\n";
    script += "insert into V set name ='c';\n";
    script += "insert into V set name ='d';\n";
    script += "commit;\n";
    script += "select from v where name = ?;";

    YTResultSet result = db.execute("sql", script, "a");
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));

    Assert.assertEquals(1, list.size());
  }

  @Test
  public void testWithNamedParams() {
    String script = "begin;\n";
    script += "insert into V set name ='a';\n";
    script += "insert into V set name ='b';\n";
    script += "insert into V set name ='c';\n";
    script += "insert into V set name ='d';\n";
    script += "commit;\n";
    script += "select from v where name = :name;";

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "a");

    YTResultSet result = db.execute("sql", script, params);
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));

    Assert.assertEquals(1, list.size());
  }

  @Test
  public void testMultipleCreateEdgeOnTheSameLet() {
    String script = "begin;";
    script += "let $v1 = create vertex v set name = 'Foo';";
    script += "let $v2 = create vertex v set name = 'Bar';";
    script += "create edge from $v1 to $v2;";
    script += "let $v3 = create vertex v set name = 'Baz';";
    script += "create edge from $v1 to $v3;";
    script += "commit;";

    YTResultSet result = db.execute("sql", script);
    result.close();

    result = db.query("SELECT expand(out()) FROM V WHERE name ='Foo'");
    Assert.assertEquals(2, result.stream().count());
    result.close();
  }
}
