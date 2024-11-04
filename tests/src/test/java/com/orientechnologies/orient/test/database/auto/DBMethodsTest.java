package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/15/14
 */
@Test
public class DBMethodsTest extends DocumentDBBaseTest {
  @Parameters(value = "remote")
  public DBMethodsTest(boolean remote) {
    super(remote);
  }

  public void testAddCluster() {
    database.addCluster("addClusterTest");

    Assert.assertTrue(database.existsCluster("addClusterTest"));
    Assert.assertTrue(database.existsCluster("addclUstertESt"));
  }
}
