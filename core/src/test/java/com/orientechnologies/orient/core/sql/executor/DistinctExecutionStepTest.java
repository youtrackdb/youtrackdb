package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DistinctExecutionStepTest extends DBTestBase {

  @Test
  public void test() {
    var ctx = new OBasicCommandContext();
    ctx.setDatabase(db);

    DistinctExecutionStep step = new DistinctExecutionStep(ctx, false);

    AbstractExecutionStep prev =
        new AbstractExecutionStep(ctx, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                YTResultInternal item = new YTResultInternal(ctx.getDatabase());
                item.setProperty("name", i % 2 == 0 ? "foo" : "bar");
                result.add(item);
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(prev);
    OExecutionStream res = step.start(ctx);
    Assert.assertTrue(res.hasNext(ctx));
    res.next(ctx);
    Assert.assertTrue(res.hasNext(ctx));
    res.next(ctx);
    Assert.assertFalse(res.hasNext(ctx));
  }
}
