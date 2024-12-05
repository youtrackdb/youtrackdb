package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OSleepStatementExecutionTest extends DBTestBase {

  @Test
  public void testBasic() {
    long begin = System.currentTimeMillis();
    YTResultSet result = db.command("sleep 1000");
    Assert.assertTrue(System.currentTimeMillis() - begin >= 1000);
    printExecutionPlan(null, result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    YTResult item = result.next();
    Assert.assertEquals("sleep", item.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
  }
}
