package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal.ATTRIBUTES;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OAlterDatabaseStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testSetProperty() {
    Object previousValue = db.get(ATTRIBUTES.MINIMUMCLUSTERS);

    OResultSet result = db.command("alter database MINIMUMCLUSTERS 12");

    Object currentValue = db.get(ATTRIBUTES.MINIMUMCLUSTERS);

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(previousValue, next.getProperty("oldValue"));
    Assert.assertEquals(12, currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }

  @Test
  public void testSetCustom() {
    List<OStorageEntryConfiguration> previousCustoms =
        (List<OStorageEntryConfiguration>) db.get(ATTRIBUTES.CUSTOM);
    Object prev = null;
    for (OStorageEntryConfiguration entry : previousCustoms) {
      if (entry.name.equals("foo")) {
        prev = entry.value;
      }
    }
    OResultSet result = db.command("alter database custom foo = 'bar'");

    previousCustoms = (List<OStorageEntryConfiguration>) db.get(ATTRIBUTES.CUSTOM);
    Object after = null;
    for (OStorageEntryConfiguration entry : previousCustoms) {
      if (entry.name.equals("foo")) {
        after = entry.value;
      }
    }

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals(prev, next.getProperty("oldValue"));
    Assert.assertEquals("bar", after);
    Assert.assertEquals("bar", next.getProperty("newValue"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
