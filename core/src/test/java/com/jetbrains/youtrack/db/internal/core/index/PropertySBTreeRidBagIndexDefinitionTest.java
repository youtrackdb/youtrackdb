package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class PropertySBTreeRidBagIndexDefinitionTest extends
    PropertyRidBagAbstractIndexDefinition {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
    builder.addGlobalConfigurationParameter(
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);

    return builder.build();
  }

  @Override
  void assertEmbedded(RidBag ridBag) {
    Assert.assertFalse(ridBag.isEmbedded());
  }
}
