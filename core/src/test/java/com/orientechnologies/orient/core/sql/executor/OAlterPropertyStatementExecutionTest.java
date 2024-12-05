package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OAlterPropertyStatementExecutionTest extends DBTestBase {

  @Test
  public void testSetProperty() {
    String className = "testSetProperty";
    YTClass clazz = db.getMetadata().getSchema().createClass(className);
    YTProperty prop = clazz.createProperty(db, "name", YTType.STRING);
    prop.setMax(db, "15");

    YTResultSet result = db.command("alter property " + className + ".name max 30");
    printExecutionPlan(null, result);
    Object currentValue = prop.getMax();

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("15", next.getProperty("oldValue"));
    Assert.assertEquals("30", currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }

  @Test
  public void testSetCustom() {
    String className = "testSetCustom";
    YTClass clazz = db.getMetadata().getSchema().createClass(className);
    YTProperty prop = clazz.createProperty(db, "name", YTType.STRING);
    prop.setCustom(db, "foo", "bar");

    YTResultSet result = db.command("alter property " + className + ".name custom foo='baz'");
    printExecutionPlan(null, result);
    Object currentValue = prop.getCustom("foo");

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("bar", next.getProperty("oldValue"));
    Assert.assertEquals("baz", currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }
}
