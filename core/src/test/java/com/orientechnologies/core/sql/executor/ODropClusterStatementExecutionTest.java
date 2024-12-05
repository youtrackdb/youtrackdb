package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropClusterStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String cluster = "testPlain";
    db.getStorage().addCluster(db, cluster);

    Assert.assertTrue(db.getClusterIdByName(cluster) > 0);
    YTResultSet result = db.command("drop cluster " + cluster);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop cluster", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertTrue(db.getClusterIdByName(cluster) < 0);
  }

  @Test
  public void testDropClusterIfExists() {
    String cluster = "testDropClusterIfExists";
    db.getStorage().addCluster(db, cluster);

    Assert.assertTrue(db.getClusterIdByName(cluster) > 0);
    YTResultSet result = db.command("drop cluster " + cluster + " IF EXISTS");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop cluster", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    Assert.assertTrue(db.getClusterIdByName(cluster) < 0);

    result = db.command("drop cluster " + cluster + " IF EXISTS");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
