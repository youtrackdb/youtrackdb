package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CheckRecordTypeStepTest extends TestUtilsFixture {

  @Test
  public void shouldCheckRecordsOfOneType() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    String className = createClassInstance().getName();
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, className, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(new YTResultInternal(ctx.getDatabase(), new YTDocument(className)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test
  public void shouldCheckRecordsOfSubclasses() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    YTClass parentClass = createClassInstance();
    YTClass childClass = createChildClassInstance(parentClass);
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, parentClass.getName(), false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new YTResultInternal(ctx.getDatabase(),
                        new YTDocument(i % 2 == 0 ? parentClass : childClass)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);
    Assert.assertEquals(10, result.stream(context).count());
    Assert.assertFalse(result.hasNext(context));
  }

  @Test(expected = YTCommandExecutionException.class)
  public void shouldThrowExceptionWhenTypeIsDifferent() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    String firstClassName = createClassInstance().getName();
    String secondClassName = createClassInstance().getName();
    CheckRecordTypeStep step = new CheckRecordTypeStep(context, firstClassName, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new YTResultInternal(ctx.getDatabase(),
                        new YTDocument(i % 2 == 0 ? firstClassName : secondClassName)));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);
    while (result.hasNext(context)) {
      result.next(context);
    }
  }
}
