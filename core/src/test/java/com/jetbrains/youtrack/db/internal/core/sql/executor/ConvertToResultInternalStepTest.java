package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OBasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ConvertToResultInternalStepTest extends TestUtilsFixture {

  private static final String STRING_PROPERTY = "stringPropertyName";
  private static final String INTEGER_PROPERTY = "integerPropertyName";
  private final List<EntityImpl> documents = new ArrayList<>();

  @Test
  public void shouldConvertUpdatableResult() {
    OCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    ConvertToResultInternalStep step = new ConvertToResultInternalStep(context, false);
    AbstractExecutionStep previous =
        new AbstractExecutionStep(context, false) {
          boolean done = false;

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
            List<YTResult> result = new ArrayList<>();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                EntityImpl document = new EntityImpl();
                document.setProperty(STRING_PROPERTY, RandomStringUtils.randomAlphanumeric(10));
                document.setProperty(INTEGER_PROPERTY, new Random().nextInt());
                documents.add(document);
                YTUpdatableResult item = new YTUpdatableResult(db, document);
                result.add(item);
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
      if (!(currentItem.getClass().equals(YTResultInternal.class))) {
        Assert.fail("There is an item in result set that is not an instance of YTResultInternal");
      }
      if (!currentItem
          .getEntity()
          .get()
          .getProperty(STRING_PROPERTY)
          .equals(documents.get(counter).getProperty(STRING_PROPERTY))) {
        Assert.fail("String EntityImpl property inside YTResult instance is not preserved");
      }
      if (!currentItem
          .getEntity()
          .get()
          .getProperty(INTEGER_PROPERTY)
          .equals(documents.get(counter).getProperty(INTEGER_PROPERTY))) {
        Assert.fail("Integer EntityImpl property inside YTResult instance is not preserved");
      }
      counter++;
    }
  }
}
