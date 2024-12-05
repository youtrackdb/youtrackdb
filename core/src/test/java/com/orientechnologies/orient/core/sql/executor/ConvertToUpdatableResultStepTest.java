package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ConvertToUpdatableResultStepTest extends TestUtilsFixture {

  private static final String STRING_PROPERTY = "stringPropertyName";
  private static final String INTEGER_PROPERTY = "integerPropertyName";
  private final List<YTEntityImpl> documents = new ArrayList<>();

  @Test
  public void shouldConvertUpdatableResult() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    ConvertToUpdatableResultStep step = new ConvertToUpdatableResultStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                YTEntityImpl document = new YTEntityImpl();
                document.setProperty(STRING_PROPERTY, RandomStringUtils.randomAlphanumeric(10));
                document.setProperty(INTEGER_PROPERTY, new Random().nextInt());
                documents.add(document);
                result.add(new YTResultInternal(ctx.getDatabase(), document));
              }
              done = true;
            }
            return OExecutionStream.resultIterator(result.iterator());
          }
        };

    step.setPrevious(previous);
    OExecutionStream result = step.start(context);

    int counter = 0;
    while (result.hasNext(context)) {
      YTResult currentItem = result.next(context);
      if (!(currentItem.getClass().equals(YTUpdatableResult.class))) {
        Assert.fail("There is an item in result set that is not an instance of YTUpdatableResult");
      }
      if (!currentItem
          .getEntity()
          .get()
          .getProperty(STRING_PROPERTY)
          .equals(documents.get(counter).getProperty(STRING_PROPERTY))) {
        Assert.fail("String YTEntityImpl property inside YTResult instance is not preserved");
      }
      if (!currentItem
          .getEntity()
          .get()
          .getProperty(INTEGER_PROPERTY)
          .equals(documents.get(counter).getProperty(INTEGER_PROPERTY))) {
        Assert.fail("Integer YTEntityImpl property inside YTResult instance is not preserved");
      }
      counter++;
    }
  }
}
