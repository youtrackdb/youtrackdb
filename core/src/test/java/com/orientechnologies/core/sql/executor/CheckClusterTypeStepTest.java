package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
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
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLASS_CLUSTER_NAME, clazz.getName(), context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = YTCommandExecutionException.class)
  public void shouldThrowExceptionWhenClusterIsWrong() {
    db.addCluster(CLUSTER_NAME);
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClusterTypeStep step =
        new CheckClusterTypeStep(CLUSTER_NAME, createClassInstance().getName(), context, false);

    step.start(context);
  }
}
