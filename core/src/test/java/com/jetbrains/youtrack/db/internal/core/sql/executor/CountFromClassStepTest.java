package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.OBasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
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
      EntityImpl document = new EntityImpl(className);
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
