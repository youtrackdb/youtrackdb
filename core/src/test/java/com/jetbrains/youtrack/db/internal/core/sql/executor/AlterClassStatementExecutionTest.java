package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterClassStatementExecutionTest extends DbTestBase {

  @Test
  public void testName1() {
    var className = "testName1";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command("alter class " + className + " name " + className + "_new");
    Assert.assertNull(schema.getClass(className));
    Assert.assertNotNull(schema.getClass(className + "_new"));
    result.close();
  }

  @Test
  public void testName2() {
    var className = "testName2";
    Schema schema = db.getMetadata().getSchema();
    var e = schema.getClass("E");
    if (e == null) {
      schema.createClass("E");
    }
    schema.createClass(className, e);
    try {
      db.command("alter class " + className + " name " + className + "_new");
      Assert.fail();
    } catch (CommandExecutionException ex) {

    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertNotNull(schema.getClass(className));
    Assert.assertNull(schema.getClass(className + "_new"));
  }

  @Test
  public void testShortName() {
    var className = "testShortName";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command(
        "alter class " + className + " shortname " + className + "_new");
    var clazz = schema.getClass(className);
    Assert.assertEquals(className + "_new", clazz.getShortName());
    result.close();
  }

  @Test
  public void testAddCluster() {
    var className = "testAddCluster";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result =
        db.command("alter class " + className + " add_cluster " + className + "_new");
    var clazz = schema.getClass(className);
    var found = false;
    for (var i : clazz.getClusterIds()) {
      var clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertTrue(found);
  }

  @Test
  public void testRemoveCluster() {
    var className = "testRemoveCluster";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result =
        db.command("alter class " + className + " add_cluster " + className + "_new");
    var clazz = schema.getClass(className);
    var found = false;
    for (var i : clazz.getClusterIds()) {
      var clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertTrue(found);

    result = db.command("alter class " + className + " remove_cluster " + className + "_new");
    clazz = schema.getClass(className);
    found = false;
    for (var i : clazz.getClusterIds()) {
      var clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertFalse(found);
  }

  @Test
  public void testSuperclass() {
    var className = "testSuperclass_sub";
    var superclassName = "testSuperclass_super";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var superclass = schema.createClass(superclassName);
    var result = db.command("alter class " + className + " superclass " + superclassName);
    Assert.assertTrue(schema.getClass(className).getSuperClasses().contains(superclass));
    result.close();
  }

  @Test
  public void testSuperclasses() {
    var className = "testSuperclasses_sub";
    var superclassName = "testSuperclasses_super1";
    var superclassName2 = "testSuperclasses_super2";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var superclass = schema.createClass(superclassName);
    var superclass2 = schema.createClass(superclassName2);
    var result =
        db.command(
            "alter class "
                + className
                + " superclasses "
                + superclassName
                + ", "
                + superclassName2);
    Assert.assertTrue(schema.getClass(className).getSuperClasses().contains(superclass));
    Assert.assertTrue(schema.getClass(className).getSuperClasses().contains(superclass2));
    result.close();
  }

  @Test
  public void testStrictmode() {
    var className = "testStrictmode";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command("alter class " + className + " strict_mode true");
    var clazz = schema.getClass(className);
    Assert.assertTrue(clazz.isStrictMode());
    result.close();
  }

  @Test
  public void testCustom() {
    var className = "testCustom";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command("alter class " + className + " custom foo = 'bar'");
    var clazz = schema.getClass(className);
    Assert.assertEquals("bar", clazz.getCustom("foo"));
    result.close();
  }

  @Test
  public void testCustom2() {
    var className = "testCustom2";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command("alter class " + className + " custom foo = ?", "bar");
    var clazz = schema.getClass(className);
    Assert.assertEquals("bar", clazz.getCustom("foo"));
    result.close();
  }

  @Test
  public void testAbstract() {
    var className = "testAbstract";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    var result = db.command("alter class " + className + " abstract true");
    var clazz = schema.getClass(className);
    Assert.assertTrue(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testUnsafe1() {
    var className = "testUnsafe1";
    Schema schema = db.getMetadata().getSchema();
    var e = schema.getClass("E");
    if (e == null) {
      e = schema.createClass("E");
    }
    schema.createClass(className, e);
    try {
      db.command("alter class " + className + " name " + className + "_new");
      Assert.fail();
    } catch (CommandExecutionException ex) {

    }
    var result =
        db.command("alter class " + className + " name " + className + "_new unsafe");
    Assert.assertNull(schema.getClass(className));
    Assert.assertNotNull(schema.getClass(className + "_new"));
    result.close();
  }
}
