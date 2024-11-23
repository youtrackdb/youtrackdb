package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropClassStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {
    String className = "testPlain";
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    OResultSet result = db.command("drop class " + className);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testUnsafe() {

    String className = "testUnsafe";
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    schema.createClass(className, v);

    db.begin();
    db.command("insert into " + className + " set foo = 'bar'");
    db.commit();
    try {
      db.command("drop class " + className).close();
      Assert.fail();
    } catch (OCommandExecutionException ex1) {
    } catch (Exception ex2) {
      Assert.fail();
    }
    OResultSet result = db.command("drop class " + className + " unsafe");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }

  @Test
  public void testIfExists() {
    String className = "testIfExists";
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    OResultSet result = db.command("drop class " + className + " if exists");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
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
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    Assert.assertNotNull(schema.getClass(className));

    OResultSet result = db.command("drop class ?", className);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    Assert.assertNull(schema.getClass(className));
  }
}
