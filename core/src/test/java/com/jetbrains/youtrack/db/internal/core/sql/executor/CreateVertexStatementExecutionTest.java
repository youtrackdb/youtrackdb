package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.Schema;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateVertexStatementExecutionTest extends DbTestBase {

  @Test
  public void testInsertSet() {
    var className = "testInsertSet";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    var result = db.command("create vertex " + className + " set name = 'name1'");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertSetNoVertex() {
    var className = "testInsertSetNoVertex";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    try {
      var result = db.command("create vertex " + className + " set name = 'name1'");
      Assert.fail();
    } catch (CommandExecutionException e1) {
    } catch (Exception e2) {
      Assert.fail();
    }
  }

  @Test
  public void testInsertValue() {
    var className = "testInsertValue";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    var result =
        db.command("create vertex " + className + "  (name, surname) values ('name1', 'surname1')");
    db.commit();
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue2() {
    var className = "testInsertValue2";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    var result =
        db.command(
            "create vertex "
                + className
                + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");
    db.commit();

    printExecutionPlan(result);

    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name" + (i + 1), item.getProperty("name"));
      Assert.assertEquals("surname" + (i + 1), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = db.query("select from " + className);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
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
    var className = "testContent";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className, schema.getClass("V"));

    db.begin();
    var result =
        db.command(
            "create vertex " + className + " content {'name':'name1', 'surname':'surname1'}");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
