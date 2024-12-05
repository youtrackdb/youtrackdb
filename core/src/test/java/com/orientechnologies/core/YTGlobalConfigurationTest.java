package com.orientechnologies.core;

import com.orientechnologies.core.config.YTGlobalConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class YTGlobalConfigurationTest {

  @Test
  public void testReadFromEnv() {

    Assert.assertEquals(
        "ORIENTDB_DISTRIBUTED", YTGlobalConfiguration.getEnvKey(YTGlobalConfiguration.DISTRIBUTED));

    Assert.assertEquals(
        "ORIENTDB_DISTRIBUTED_NODE_NAME",
        YTGlobalConfiguration.getEnvKey(YTGlobalConfiguration.DISTRIBUTED_NODE_NAME));
  }

  /**
   * YTGlobalConfiguration.DISTRIBUTED has no explicit "section"
   */
  @Test
  public void testDumpConfiguraiton() {
    final OutputStream os = new ByteArrayOutputStream();
    YTGlobalConfiguration.dumpConfiguration(new PrintStream(os));
    Assert.assertTrue(os.toString().contains(YTGlobalConfiguration.DISTRIBUTED.getKey()));
  }
}
