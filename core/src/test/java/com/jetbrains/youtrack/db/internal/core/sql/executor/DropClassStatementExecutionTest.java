package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DropClassStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    var className = "testPlain";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    var result = db.command("drop class " + className);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testUnsafe() {

    var className = "testUnsafe";
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    schema.createClass(className, v);

    db.begin();
    db.command("insert into " + className + " set foo = 'bar'");
    db.commit();
    try {
      db.command("drop class " + className).close();
      Assert.fail();
    } catch (CommandExecutionException ex1) {
    } catch (Exception ex2) {
      Assert.fail();
    }
    var result = db.command("drop class " + className + " unsafe");
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testIfExists() {
    var className = "testIfExists";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    var result = db.command("drop class " + className + " if exists");
    Assert.assertTrue(result.hasNext());
    var next = result.next();
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
    var className = "testParam";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    var result = db.command("drop class ?", className);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }
}
