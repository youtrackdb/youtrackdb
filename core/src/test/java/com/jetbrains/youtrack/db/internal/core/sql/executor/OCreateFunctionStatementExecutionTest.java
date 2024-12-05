package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateFunctionStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    db.begin();
    YTResultSet result =
        db.command(
            "CREATE FUNCTION " + name + " \"return a + b;\" PARAMETERS [a,b] language javascript");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals(name, next.getProperty("functionName"));
    result.close();
    db.commit();

    result = db.query("select " + name + "('foo', 'bar') as sum");
    Assert.assertTrue(result.hasNext());
    next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals("foobar", next.getProperty("sum"));
    result.close();
  }
}
