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
public class CheckClassTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckSubclasses() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    YTClass parentClass = createClassInstance();
    YTClass childClass = createChildClassInstance(parentClass);
    CheckClassTypeStep step =
        new CheckClassTypeStep(childClass.getName(), parentClass.getName(), context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test
  public void shouldCheckOneType() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    String className = createClassInstance().getName();
    CheckClassTypeStep step = new CheckClassTypeStep(className, className, context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = YTCommandExecutionException.class)
  public void shouldThrowExceptionWhenClassIsNotParent() {
    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CheckClassTypeStep step =
        new CheckClassTypeStep(
            createClassInstance().getName(), createClassInstance().getName(), context, false);

    step.start(context);
  }
}
