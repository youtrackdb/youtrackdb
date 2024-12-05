package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OIdentifier;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CountFromClassStepTest extends TestUtilsFixture {

  private static final String ALIAS = "size";

  @Test
  public void shouldCountRecordsOfClass() {
    String className = createClassInstance().getName();
    for (int i = 0; i < 20; i++) {
      db.begin();
      YTEntityImpl document = new YTEntityImpl(className);
      document.save();
      db.commit();
    }

    OIdentifier classIdentifier = new OIdentifier(className);

    OBasicCommandContext context = new OBasicCommandContext();
    context.setDatabase(db);
    CountFromClassStep step = new CountFromClassStep(classIdentifier, ALIAS, context, false);

    OExecutionStream result = step.start(context);
    Assert.assertEquals(20, (long) result.next(context).getProperty(ALIAS));
    Assert.assertFalse(result.hasNext(context));
  }
}
