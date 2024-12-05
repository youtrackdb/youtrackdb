package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropPropertyStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String className = "testPlain";
    String propertyName = "foo";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className).createProperty(db, propertyName, YTType.STRING);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    YTResultSet result = db.command("drop property " + className + "." + propertyName);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop property", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertNull(schema.getClass(className).getProperty(propertyName));
  }

  @Test
  public void testDropIndexForce() {
    String className = "testDropIndexForce";
    String propertyName = "foo";
    YTSchema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(db, propertyName, YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.NOTUNIQUE);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    YTResultSet result = db.command("drop property " + className + "." + propertyName + " force");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }

    Assert.assertFalse(result.hasNext());

    result.close();

    Assert.assertNull(schema.getClass(className).getProperty(propertyName));
  }

  @Test
  public void testDropIndex() {

    String className = "testDropIndex";
    String propertyName = "foo";
    YTSchema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(db, propertyName, YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.NOTUNIQUE);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    try {
      db.command("drop property " + className + "." + propertyName);
      Assert.fail();
    } catch (YTCommandExecutionException e) {
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
