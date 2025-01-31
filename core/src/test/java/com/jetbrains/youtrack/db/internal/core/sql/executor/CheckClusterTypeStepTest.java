package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
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
    var clazz = createClassInstance().addCluster(db, CLASS_CLUSTER_NAME);
    var context = new BasicCommandContext();
    context.setDatabase(db);
    var step =
        new CheckClusterTypeStep(CLASS_CLUSTER_NAME, clazz.getName(), context, false);

    var result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = CommandExecutionException.class)
  public void shouldThrowExceptionWhenClusterIsWrong() {
    db.addCluster(CLUSTER_NAME);
    var context = new BasicCommandContext();
    context.setDatabase(db);
    var step =
        new CheckClusterTypeStep(CLUSTER_NAME, createClassInstance().getName(), context, false);

    step.start(context);
  }
}
