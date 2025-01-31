package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ConsoleStatementExecutionTest extends DbTestBase {

  @Test
  public void testError() {
    var result = db.command("console.error 'foo bar'");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("error", item.getProperty("level"));
    Assert.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testLog() {
    var result = db.command("console.log 'foo bar'");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("log", item.getProperty("level"));
    Assert.assertEquals("foo bar", item.getProperty("message"));
  }

  @Test
  public void testInvalidLevel() {
    try {
      db.command("console.bla 'foo bar'");
      Assert.fail();
    } catch (CommandExecutionException x) {

    } catch (Exception x2) {
      Assert.fail();
    }
  }
}
