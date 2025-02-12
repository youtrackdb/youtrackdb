package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CheckClassTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckSubclasses() {
    var context = new BasicCommandContext();
    context.setDatabaseSession(session);
    var parentClass = createClassInstance();
    var childClass = createChildClassInstance(parentClass);
    var step =
        new CheckClassTypeStep(childClass.getName(session), parentClass.getName(session), context,
            false);

    var result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test
  public void shouldCheckOneType() {
    var context = new BasicCommandContext();
    context.setDatabaseSession(session);
    var className = createClassInstance().getName(session);
    var step = new CheckClassTypeStep(className, className, context, false);

    var result = step.start(context);
    Assert.assertEquals(0, result.stream(context).count());
  }

  @Test(expected = CommandExecutionException.class)
  public void shouldThrowExceptionWhenClassIsNotParent() {
    var context = new BasicCommandContext();
    context.setDatabaseSession(session);
    var step =
        new CheckClassTypeStep(
            createClassInstance().getName(session), createClassInstance().getName(session), context,
            false);

    step.start(context);
  }
}
