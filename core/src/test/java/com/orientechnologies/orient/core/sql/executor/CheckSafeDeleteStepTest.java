package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CheckSafeDeleteStepTest extends TestUtilsFixture {

  private static final String VERTEX_CLASS_NAME = "VertexTestClass";
  private static final String EDGE_CLASS_NAME = "EdgeTestClass";

  private final String className;

  public CheckSafeDeleteStepTest(String className) {
    this.className = className;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> documentTypes() {
    return Arrays.asList(
        new Object[][]{
            {VERTEX_CLASS_NAME}, {EDGE_CLASS_NAME},
        });
  }

  @Test(expected = YTCommandExecutionException.class)
  public void shouldNotDeleteVertexAndEdge() {
    OCommandContext context = new OBasicCommandContext();
    switch (className) {
      case VERTEX_CLASS_NAME:
        db.createVertexClass(VERTEX_CLASS_NAME);
        break;
      case EDGE_CLASS_NAME:
        db.createEdgeClass(EDGE_CLASS_NAME);
        break;
    }

    CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            String simpleClassName = createClassInstance().getName();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new YTResultInternal(db,
                        new YTEntityImpl(i % 2 == 0 ? simpleClassName : className)));
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

  @Test
  public void shouldSafelyDeleteRecord() {
    OCommandContext context = new OBasicCommandContext();
    CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                result.add(
                    new YTResultInternal(db, new YTEntityImpl(createClassInstance().getName())));
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
}
