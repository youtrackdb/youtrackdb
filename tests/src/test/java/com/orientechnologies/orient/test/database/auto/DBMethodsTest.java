package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 9/15/14
 */
@Test
public class DBMethodsTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public DBMethodsTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(YTGlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  public void testAddCluster() {
    database.addCluster("addClusterTest");

    Assert.assertTrue(database.existsCluster("addClusterTest"));
    Assert.assertTrue(database.existsCluster("addclUstertESt"));
  }
}
