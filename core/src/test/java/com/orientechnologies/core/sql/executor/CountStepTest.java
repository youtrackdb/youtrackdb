package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CountStepTest extends DBTestBase {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String COUNT_PROPERTY_NAME = "count";

  @Test
  public void shouldCountRecords() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);

    CountStep step = new CountStep(context, false);

    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 100; i++) {
                YTResultInternal item = new YTResultInternal(ctx.getDatabase());
                item.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                result.add(item);
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);
    Assert.assertEquals(100, (long) result.next(context).getProperty(COUNT_PROPERTY_NAME));
    Assert.assertFalse(result.hasNext(context));
  }
}
