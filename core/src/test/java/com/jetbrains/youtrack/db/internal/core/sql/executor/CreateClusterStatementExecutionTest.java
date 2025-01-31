package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateClusterStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    var clusterName = "testPlain";
    var result = db.command("create cluster " + clusterName);
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    result.close();
  }

  @Test
  public void testExisting() {
    var clazz = db.getMetadata().getSchema().createClass("testExisting");
    var clusterName = db.getClusterNameById(clazz.getClusterIds()[0]);
    try {
      db.command("create cluster " + clusterName);
      Assert.fail();
    } catch (CommandExecutionException ex) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testWithNumber() {
    var clusterName = "testWithNumber";
    var result = db.command("create cluster " + clusterName + " id 1000");
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertNotNull(db.getClusterNameById(1000));

    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 1000, next.getProperty("requestedId"));
    result.close();
  }

  @Test
  public void testBlob() {
    var clusterName = "testBlob";
    var result = db.command("create blob cluster " + clusterName);
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertTrue(db.getStorage().getClusterIdByName(clusterName) >= 0);
    // TODO test that it's a blob cluster
    result.close();
  }

  @Test
  public void testIfNotExists() {
    var clusterName = "testIfNotExists";
    var result = db.command("create cluster " + clusterName + " IF NOT EXISTS id 2000");
    Assert.assertTrue(db.getClusterIdByName(clusterName) > 0);
    Assert.assertNotNull(db.getClusterNameById(2000));

    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 2000, next.getProperty("requestedId"));
    result.close();

    result = db.command("create cluster " + clusterName + " IF NOT EXISTS id 1000");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
