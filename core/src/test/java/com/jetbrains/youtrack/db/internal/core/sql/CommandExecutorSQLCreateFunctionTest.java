package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CommandExecutorSQLCreateFunctionTest extends DBTestBase {

  @Test
  public void testCreateFunction() {
    db.begin();
    db.command(
            "CREATE FUNCTION testCreateFunction \"return 'hello '+name;\" PARAMETERS [name]"
                + " IDEMPOTENT true LANGUAGE Javascript")
        .close();
    db.commit();

    YTResultSet result = db.command("select testCreateFunction('world') as name");
    Assert.assertEquals(result.next().getProperty("name"), "hello world");
    Assert.assertFalse(result.hasNext());
  }
}
