package com.orientechnologies.core.index;

import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class YTPropertySBTreeRidBagIndexDefinitionTest extends
    YTPropertyRidBagAbstractIndexDefinition {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
    builder.addConfig(YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);

    return builder.build();
  }

  @Override
  void assertEmbedded(RidBag ridBag) {
    Assert.assertFalse(ridBag.isEmbedded());
  }
}
