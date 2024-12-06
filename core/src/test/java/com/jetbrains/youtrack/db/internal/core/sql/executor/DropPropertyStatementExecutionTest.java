package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DropPropertyStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    String className = "testPlain";
    String propertyName = "foo";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className).createProperty(db, propertyName, PropertyType.STRING);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    ResultSet result = db.command("drop property " + className + "." + propertyName);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertEquals("drop property", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertNull(schema.getClass(className).getProperty(propertyName));
  }

  @Test
  public void testDropIndexForce() {
    String className = "testDropIndexForce";
    String propertyName = "foo";
    Schema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(db, propertyName, PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    ResultSet result = db.command("drop property " + className + "." + propertyName + " force");
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
    Schema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(db, propertyName, PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    try {
      db.command("drop property " + className + "." + propertyName);
      Assert.fail();
    } catch (CommandExecutionException e) {
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
