package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OBasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
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
