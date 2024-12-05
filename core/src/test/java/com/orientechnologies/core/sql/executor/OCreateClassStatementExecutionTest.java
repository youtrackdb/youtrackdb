package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateClassStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String className = "testPlain";
    YTResultSet result = db.command("create class " + className);
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testAbstract() {
    String className = "testAbstract";
    YTResultSet result = db.command("create class " + className + " abstract ");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertTrue(clazz.isAbstract());
    result.close();
  }

  @Test
  public void testCluster() {
    String className = "testCluster";
    YTResultSet result = db.command("create class " + className + " cluster 1235, 1236, 1255");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(3, clazz.getClusterIds().length);
    result.close();
  }

  @Test
  public void testClusters() {
    String className = "testClusters";
    YTResultSet result = db.command("create class " + className + " clusters 32");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    Assert.assertFalse(clazz.isAbstract());
    Assert.assertEquals(32, clazz.getClusterIds().length);
    result.close();
  }

  @Test
  public void testIfNotExists() {
    String className = "testIfNotExists";
    YTResultSet result = db.command("create class " + className + " if not exists");
    YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    result.close();

    result = db.command("create class " + className + " if not exists");
    clazz = schema.getClass(className);
    Assert.assertNotNull(clazz);
    result.close();
  }
}
