package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.sql.executor.FetchFromRidsStep;
import com.orientechnologies.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.core.sql.executor.OSelectExecutionPlan;
import com.orientechnologies.core.sql.executor.ParallelExecStep;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 *
 */
public class ParallelExecStepTest {

  @Test
  public void test() {
    OCommandContext ctx = new OBasicCommandContext();
    List<OInternalExecutionPlan> subPlans = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      FetchFromRidsStep step0 =
          new FetchFromRidsStep(Collections.singleton(new YTRecordId(12, i)), ctx, false);
      FetchFromRidsStep step1 =
          new FetchFromRidsStep(Collections.singleton(new YTRecordId(12, i)), ctx, false);
      OInternalExecutionPlan plan = new OSelectExecutionPlan(ctx);
      plan.getSteps().add(step0);
      plan.getSteps().add(step1);
      subPlans.add(plan);
    }

    ParallelExecStep step = new ParallelExecStep(subPlans, ctx, false);

    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    plan.getSteps()
        .add(new FetchFromRidsStep(Collections.singleton(new YTRecordId(12, 100)), ctx, false));
    plan.getSteps().add(step);
    plan.getSteps()
        .add(new FetchFromRidsStep(Collections.singleton(new YTRecordId(12, 100)), ctx, false));
  }
}
