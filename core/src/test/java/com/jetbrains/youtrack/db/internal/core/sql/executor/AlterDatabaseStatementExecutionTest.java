package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterDatabaseStatementExecutionTest extends DbTestBase {

  @Test
  public void testSetProperty() {
    Object previousValue = db.get(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS);

    ResultSet result = db.command("alter database MINIMUM_CLUSTERS 12");

    Object currentValue = db.get(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(previousValue, next.getProperty("oldValue"));
    Assert.assertEquals(12, currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }
}
