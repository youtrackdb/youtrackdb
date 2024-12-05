package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.OExecutionPlan;
import com.orientechnologies.core.sql.executor.OSelectExecutionPlan;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OExplainStatementExecutionTest extends DBTestBase {

  @Test
  public void testExplainSelectNoTarget() {
    YTResultSet result = db.query("explain select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertNotNull(next.getProperty("executionPlan"));
    Assert.assertNotNull(next.getProperty("executionPlanAsString"));

    Optional<OExecutionPlan> plan = result.getExecutionPlan();
    Assert.assertTrue(plan.isPresent());
    Assert.assertTrue(plan.get() instanceof OSelectExecutionPlan);

    result.close();
  }
}
