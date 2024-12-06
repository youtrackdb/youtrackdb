package com.jetbrains.youtrack.db.internal.core;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GlobalConfigurationTest {

  @Test
  public void testReadFromEnv() {

    Assert.assertEquals(
        "YOUTRACKDB_DISTRIBUTED", GlobalConfiguration.getEnvKey(GlobalConfiguration.DISTRIBUTED));

    Assert.assertEquals(
        "YOUTRACKDB_DISTRIBUTED_NODE_NAME",
        GlobalConfiguration.getEnvKey(GlobalConfiguration.DISTRIBUTED_NODE_NAME));
  }

  /**
   * GlobalConfiguration.DISTRIBUTED has no explicit "section"
   */
  @Test
  public void testDumpConfiguraiton() {
    final OutputStream os = new ByteArrayOutputStream();
    GlobalConfiguration.dumpConfiguration(new PrintStream(os));
    Assert.assertTrue(os.toString().contains(GlobalConfiguration.DISTRIBUTED.getKey()));
  }
}
