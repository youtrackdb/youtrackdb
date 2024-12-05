package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropClassStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String className = "testPlain";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    YTResultSet result = db.command("drop class " + className);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testUnsafe() {

    String className = "testUnsafe";
    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    schema.createClass(className, v);

    db.begin();
    db.command("insert into " + className + " set foo = 'bar'");
    db.commit();
    try {
      db.command("drop class " + className).close();
      Assert.fail();
    } catch (YTCommandExecutionException ex1) {
    } catch (Exception ex2) {
      Assert.fail();
    }
    YTResultSet result = db.command("drop class " + className + " unsafe");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testIfExists() {
    String className = "testIfExists";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    YTResultSet result = db.command("drop class " + className + " if exists");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));

    result = db.command("drop class " + className + " if exists");
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testParam() {
    String className = "testParam";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    YTResultSet result = db.command("drop class ?", className);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }
}
