package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OForEachBlockExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {

    String className = "testPlain";

    db.createClass(className);

    String script = "";
    script += "FOREACH ($val in [1,2,3]){\n";
    script += "  begin;insert into " + className + " set value = $val;commit;\n";
    script += "}";
    script += "SELECT FROM " + className;

    YTResultSet results = db.execute("sql", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      YTResult item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(3, tot);
    Assert.assertEquals(6, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    String className = "testReturn";

    db.createClass(className);

    String script = "";
    script += "FOREACH ($val in [1,2,3]){\n";
    script += "  begin;insert into " + className + " set value = $val;commit;\n";
    script += "  if($val = 2){\n";
    script += "    RETURN;\n";
    script += "  }\n";
    script += "}";

    YTResultSet results = db.execute("sql", script);
    results.close();
    results = db.query("SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      YTResult item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(2, tot);
    Assert.assertEquals(3, sum);
    results.close();
  }
}
