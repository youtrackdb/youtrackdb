package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ForEachBlockExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {

    var className = "testPlain";

    db.createClass(className);

    var script = "";
    script += "FOREACH ($val in [1,2,3]){\n";
    script += "  begin;insert into " + className + " set value = $val;commit;\n";
    script += "}";
    script += "SELECT FROM " + className;

    var results = db.execute("sql", script);

    var tot = 0;
    var sum = 0;
    while (results.hasNext()) {
      var item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(3, tot);
    Assert.assertEquals(6, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    var className = "testReturn";

    db.createClass(className);

    var script = "";
    script += "FOREACH ($val in [1,2,3]){\n";
    script += "  begin;insert into " + className + " set value = $val;commit;\n";
    script += "  if($val = 2){\n";
    script += "    RETURN;\n";
    script += "  }\n";
    script += "}";

    var results = db.execute("sql", script);
    results.close();
    results = db.query("SELECT FROM " + className);

    var tot = 0;
    var sum = 0;
    while (results.hasNext()) {
      var item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(2, tot);
    Assert.assertEquals(3, sum);
    results.close();
  }
}
