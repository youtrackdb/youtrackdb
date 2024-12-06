package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ExplainStatementExecutionTest extends DbTestBase {

  @Test
  public void testExplainSelectNoTarget() {
    ResultSet result = db.query("explain select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertNotNull(next.getProperty("executionPlan"));
    Assert.assertNotNull(next.getProperty("executionPlanAsString"));

    Optional<ExecutionPlan> plan = result.getExecutionPlan();
    Assert.assertTrue(plan.isPresent());
    Assert.assertTrue(plan.get() instanceof SelectExecutionPlan);

    result.close();
  }
}
