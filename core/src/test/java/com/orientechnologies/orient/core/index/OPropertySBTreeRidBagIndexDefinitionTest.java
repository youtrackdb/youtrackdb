package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.db.OxygenDBConfigBuilder;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class OPropertySBTreeRidBagIndexDefinitionTest extends
    OPropertyRidBagAbstractIndexDefinition {

  @Override
  protected OxygenDBConfig createConfig(OxygenDBConfigBuilder builder) {
    builder.addConfig(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
    builder.addConfig(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);

    return builder.build();
  }

  @Override
  void assertEmbedded(ORidBag ridBag) {
    Assert.assertFalse(ridBag.isEmbedded());
  }
}
