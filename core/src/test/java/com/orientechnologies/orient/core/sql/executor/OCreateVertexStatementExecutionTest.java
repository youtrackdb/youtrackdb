package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateVertexStatementExecutionTest extends DBTestBase {

  @Test
  public void testInsertSet() {
    String className = "testInsertSet";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    OResultSet result = db.command("create vertex " + className + " set name = 'name1'");
    db.commit();

    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertSetNoVertex() {
    String className = "testInsertSetNoVertex";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    try {
      OResultSet result = db.command("create vertex " + className + " set name = 'name1'");
      Assert.fail();
    } catch (YTCommandExecutionException e1) {
    } catch (Exception e2) {
      Assert.fail();
    }
  }

  @Test
  public void testInsertValue() {
    String className = "testInsertValue";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    OResultSet result =
        db.command("create vertex " + className + "  (name, surname) values ('name1', 'surname1')");
    db.commit();
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue2() {
    String className = "testInsertValue2";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    OResultSet result =
        db.command(
            "create vertex "
                + className
                + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");
    db.commit();

    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name" + (i + 1), item.getProperty("name"));
      Assert.assertEquals("surname" + (i + 1), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = db.query("select from " + className);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testContent() {
    String className = "testContent";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    OResultSet result =
        db.command(
            "create vertex " + className + " content {'name':'name1', 'surname':'surname1'}");
    db.commit();

    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
