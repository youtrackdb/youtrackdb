package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
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

    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    CountFromClassStep step = new CountFromClassStep(classIdentifier, ALIAS, context, false);

    ExecutionStream result = step.start(context);
    Assert.assertEquals(20, (long) result.next(context).getProperty(ALIAS));
    Assert.assertFalse(result.hasNext(context));
  }
}
