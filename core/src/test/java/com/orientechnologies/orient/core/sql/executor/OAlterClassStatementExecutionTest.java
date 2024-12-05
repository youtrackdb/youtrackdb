package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OAlterClassStatementExecutionTest extends DBTestBase {

  @Test
  public void testName1() {
    String className = "testName1";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " name " + className + "_new");
    Assert.assertNull(schema.getClass(className));
    Assert.assertNotNull(schema.getClass(className + "_new"));
    result.close();
  }

  @Test
  public void testName2() {
    String className = "testName2";
    YTSchema schema = db.getMetadata().getSchema();
    YTClass e = schema.getClass("E");
    if (e == null) {
      schema.createClass("E");
    }
    schema.createClass(className, e);
    try {
      db.command("alter class " + className + " name " + className + "_new");
      Assert.fail();
    } catch (YTCommandExecutionException ex) {

    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertNotNull(schema.getClass(className));
    Assert.assertNull(schema.getClass(className + "_new"));
  }

  @Test
  public void testShortName() {
    String className = "testShortName";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command(
        "alter class " + className + " shortname " + className + "_new");
    YTClass clazz = schema.getClass(className);
    Assert.assertEquals(className + "_new", clazz.getShortName());
    result.close();
  }

  @Test
  public void testAddCluster() {
    String className = "testAddCluster";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result =
        db.command("alter class " + className + " addcluster " + className + "_new");
    YTClass clazz = schema.getClass(className);
    boolean found = false;
    for (int i : clazz.getClusterIds()) {
      String clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertTrue(found);
  }

  @Test
  public void testRemoveCluster() {
    String className = "testRemoveCluster";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result =
        db.command("alter class " + className + " addcluster " + className + "_new");
    YTClass clazz = schema.getClass(className);
    boolean found = false;
    for (int i : clazz.getClusterIds()) {
      String clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertTrue(found);

    result = db.command("alter class " + className + " removecluster " + className + "_new");
    clazz = schema.getClass(className);
    found = false;
    for (int i : clazz.getClusterIds()) {
      String clusterName = db.getClusterNameById(i);
      if (clusterName.equalsIgnoreCase(className + "_new")) {
        found = true;
      }
    }
    result.close();
    Assert.assertFalse(found);
  }

  @Test
  public void testSuperclass() {
    String className = "testSuperclass_sub";
    String superclassName = "testSuperclass_super";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTClass superclass = schema.createClass(superclassName);
    YTResultSet result = db.command("alter class " + className + " superclass " + superclassName);
    Assert.assertTrue(schema.getClass(className).getSuperClasses().contains(superclass));
    result.close();
  }

  @Test
  public void testSuperclasses() {
    String className = "testSuperclasses_sub";
    String superclassName = "testSuperclasses_super1";
    String superclassName2 = "testSuperclasses_super2";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTClass superclass = schema.createClass(superclassName);
    YTClass superclass2 = schema.createClass(superclassName2);
    YTResultSet result =
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
  public void testOversize() {
    String className = "testOversize";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " oversize 10");
    YTClass clazz = schema.getClass(className);
    Assert.assertEquals((Object) 10.0f, clazz.getOverSize());
    result.close();
  }

  @Test
  public void testStrictmode() {
    String className = "testStrictmode";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " strictmode true");
    YTClass clazz = schema.getClass(className);
    Assert.assertTrue(clazz.isStrictMode());
    result.close();
  }

  @Test
  public void testCustom() {
    String className = "testCustom";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " custom foo = 'bar'");
    YTClass clazz = schema.getClass(className);
    Assert.assertEquals("bar", clazz.getCustom("foo"));
    result.close();
  }

  @Test
  public void testCustom2() {
    String className = "testCustom2";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " custom foo = ?", "bar");
    YTClass clazz = schema.getClass(className);
    Assert.assertEquals("bar", clazz.getCustom("foo"));
    result.close();
  }

  @Test
  public void testAbstract() {
    String className = "testAbstract";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);
    YTResultSet result = db.command("alter class " + className + " abstract true");
    YTClass clazz = schema.getClass(className);
    Assert.assertTrue(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testUnsafe1() {
    String className = "testUnsafe1";
    YTSchema schema = db.getMetadata().getSchema();
    YTClass e = schema.getClass("E");
    if (e == null) {
      e = schema.createClass("E");
    }
    schema.createClass(className, e);
    try {
      db.command("alter class " + className + " name " + className + "_new");
      Assert.fail();
    } catch (YTCommandExecutionException ex) {

    }
    YTResultSet result =
        db.command("alter class " + className + " name " + className + "_new unsafe");
    Assert.assertNull(schema.getClass(className));
    Assert.assertNotNull(schema.getClass(className + "_new"));
    result.close();
  }

  @Test
  public void testDefaultCluster() {
    String className = "testDefaultCluster";
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.createClass(className);
    int[] clusterIds = clazz.getClusterIds();
    if (clusterIds.length < 2) {
      clazz.addCluster(db, className + "_1");
      clusterIds = clazz.getClusterIds();
    }
    int currentDefault = clazz.getDefaultClusterId();
    int firstNonDefault = -1;
    for (int clusterId : clusterIds) {
      if (clusterId != currentDefault) {
        firstNonDefault = clusterId;
      }
    }

    try {
      db.command("alter class " + className + " defaultcluster " + firstNonDefault).close();
    } catch (YTCommandExecutionException ex) {
    }

    Assert.assertEquals(firstNonDefault, schema.getClass(className).getDefaultClusterId());
  }
}
