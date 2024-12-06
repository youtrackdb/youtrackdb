package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.config.StorageEntryConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal.ATTRIBUTES;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterDatabaseStatementExecutionTest extends DbTestBase {

  @Test
  public void testSetProperty() {
    Object previousValue = db.get(ATTRIBUTES.MINIMUMCLUSTERS);

    ResultSet result = db.command("alter database MINIMUMCLUSTERS 12");

    Object currentValue = db.get(ATTRIBUTES.MINIMUMCLUSTERS);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(previousValue, next.getProperty("oldValue"));
    Assert.assertEquals(12, currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }

  @Test
  public void testSetCustom() {
    List<StorageEntryConfiguration> previousCustoms =
        (List<StorageEntryConfiguration>) db.get(ATTRIBUTES.CUSTOM);
    Object prev = null;
    for (StorageEntryConfiguration entry : previousCustoms) {
      if (entry.name.equals("foo")) {
        prev = entry.value;
      }
    }
    ResultSet result = db.command("alter database custom foo = 'bar'");

    previousCustoms = (List<StorageEntryConfiguration>) db.get(ATTRIBUTES.CUSTOM);
    Object after = null;
    for (StorageEntryConfiguration entry : previousCustoms) {
      if (entry.name.equals("foo")) {
        after = entry.value;
      }
    }

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(prev, next.getProperty("oldValue"));
    Assert.assertEquals("bar", after);
    Assert.assertEquals("bar", next.getProperty("newValue"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
