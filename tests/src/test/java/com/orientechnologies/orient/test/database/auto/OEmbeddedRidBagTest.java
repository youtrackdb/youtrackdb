package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class OEmbeddedRidBagTest extends ORidBagTest {

  private int topThreshold;
  private int bottomThreshold;

  @Parameters(value = "remote")
  public OEmbeddedRidBagTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    topThreshold =
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);

    if (database.isRemote()) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, Integer.MAX_VALUE);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, Integer.MAX_VALUE);
      server.close();
    }
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.isRemote()) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, topThreshold);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, bottomThreshold);
      server.close();
    }
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue(isEmbedded);
  }
}
