package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CountFromClassStepTest extends TestUtilsFixture {

  private static final String ALIAS = "size";

  @Test
  public void shouldCountRecordsOfClass() {
    var className = createClassInstance().getName();
    for (var i = 0; i < 20; i++) {
      db.begin();
      var document = (EntityImpl) db.newEntity(className);
      document.save();
      db.commit();
    }

    var classIdentifier = new SQLIdentifier(className);

    var context = new BasicCommandContext();
    context.setDatabase(db);
    var step = new CountFromClassStep(classIdentifier, ALIAS, context, false);

    var result = step.start(context);
    Assert.assertEquals(20, (long) result.next(context).getProperty(ALIAS));
    Assert.assertFalse(result.hasNext(context));
  }
}
