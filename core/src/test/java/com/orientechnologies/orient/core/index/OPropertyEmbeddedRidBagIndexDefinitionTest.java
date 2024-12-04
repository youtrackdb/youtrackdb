package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class OPropertyEmbeddedRidBagIndexDefinitionTest extends
    OPropertyRidBagAbstractIndexDefinition {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD,
        Integer.MAX_VALUE);
    builder.addConfig(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD,
        Integer.MAX_VALUE);

    return builder.build();
  }

  @Override
  void assertEmbedded(ORidBag ridBag) {
    Assert.assertTrue(ridBag.isEmbedded());
  }
}
