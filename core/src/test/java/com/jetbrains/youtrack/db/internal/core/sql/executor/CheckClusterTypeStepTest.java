package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CheckClusterTypeStepTest extends TestUtilsFixture {

  private static final String CLASS_CLUSTER_NAME = "ClassClusterName";
  private static final String CLUSTER_NAME = "ClusterName";

  @Test
  public void shouldCheckClusterType() {
    YTClass clazz = createClassInstance().addCluster(db, CLASS_CLUSTER_NAME);
    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLASS_CLUSTER_NAME, clazz.getName(), context, false);

    ExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = YTCommandExecutionException.class)
  public void shouldThrowExceptionWhenClusterIsWrong() {
    db.addCluster(CLUSTER_NAME);
    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLUSTER_NAME, createClassInstance().getName(), context, false);

    step.start(context);
  }
}
