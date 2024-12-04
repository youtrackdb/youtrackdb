package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
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
  void assertEmbedded(ORidBag ridBag) {
    Assert.assertFalse(ridBag.isEmbedded());
  }
}
