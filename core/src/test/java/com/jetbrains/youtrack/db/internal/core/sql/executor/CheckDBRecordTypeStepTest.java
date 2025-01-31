package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CheckDBRecordTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckRecordsOfOneType() {
    CommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    var className = createClassInstance().getName();
    var step = new CheckRecordTypeStep(context, className, false);
    var previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
            List<Result> result = new ArrayList<>();
            if (!done) {
              for (var i = 0; i < 10; i++) {
                result.add(new ResultInternal(ctx.getDatabase(), db.newEntity(className)));
              }
              done = true;
            }
            return ExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    var result = step.start(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test
  public void shouldCheckRecordsOfSubclasses() {
    CommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    var parentClass = createClassInstance();
    var childClass = createChildClassInstance(parentClass);
    var step = new CheckRecordTypeStep(context, parentClass.getName(), false);
    var previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
            List<Result> result = new ArrayList<>();
            if (!done) {
              for (var i = 0; i < 10; i++) {
                result.add(
                    new ResultInternal(ctx.getDatabase(),
                        db.newEntity(i % 2 == 0 ? parentClass : childClass)));
              }
              done = true;
            }
            return ExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    var result = step.start(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test(expected = CommandExecutionException.class)
  public void shouldThrowExceptionWhenTypeIsDifferent() {
    CommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    var firstClassName = createClassInstance().getName();
    var secondClassName = createClassInstance().getName();
    var step = new CheckRecordTypeStep(context, firstClassName, false);
    var previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
            List<Result> result = new ArrayList<>();
            if (!done) {
              for (var i = 0; i < 10; i++) {
                result.add(
                    new ResultInternal(ctx.getDatabase(),
                        db.newEntity(i % 2 == 0 ? firstClassName : secondClassName)));
              }
              done = true;
            }
            return ExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    var result = step.start(context);
    while (result.hasNext(context)) {
      result.next(context);
    }
  }
}
