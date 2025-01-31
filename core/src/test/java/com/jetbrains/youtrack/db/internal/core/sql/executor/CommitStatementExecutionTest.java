package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CommitStatementExecutionTest extends DbTestBase {

  @Test
  public void testBegin() {
    Assert.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
    db.begin();
    Assert.assertFalse(db.getTransaction() == null || !db.getTransaction().isActive());
    var result = db.command("commit");
    printExecutionPlan(null, result);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertEquals("commit", item.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(db.getTransaction() == null || !db.getTransaction().isActive());
  }
}
