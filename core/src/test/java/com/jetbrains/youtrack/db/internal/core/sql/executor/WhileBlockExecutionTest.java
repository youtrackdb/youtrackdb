package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class WhileBlockExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {

    String className = "testPlain";

    db.createClass(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  begin;\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  LET $i = $i + 1;";
    script += "  commit;";
    script += "}";
    script += "SELECT FROM " + className;
    ResultSet results = db.execute("sql", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(3, tot);
    Assert.assertEquals(3, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    String className = "testReturn";

    db.createClass(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  begin;\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "   commit;\n";
    script += "  IF ($i = 1) {";
    script += "    RETURN;";
    script += "  }";
    script += "  LET $i = $i + 1;";
    script += "}";

    ResultSet results = db.execute("sql", script);
    results.close();
    results = db.query("SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      Result item = results.next();
      sum += item.<Integer>getProperty("value");
      tot++;
    }
    Assert.assertEquals(2, tot);
    Assert.assertEquals(1, sum);
    results.close();
  }
}
