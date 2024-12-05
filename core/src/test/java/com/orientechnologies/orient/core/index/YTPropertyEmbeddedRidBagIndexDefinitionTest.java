package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.db.record.ridbag.RidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class YTPropertyEmbeddedRidBagIndexDefinitionTest extends
    YTPropertyRidBagAbstractIndexDefinition {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD,
        Integer.MAX_VALUE);
    builder.addConfig(YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD,
        Integer.MAX_VALUE);

    return builder.build();
  }

  @Override
  void assertEmbedded(RidBag ridBag) {
    Assert.assertTrue(ridBag.isEmbedded());
  }
}
