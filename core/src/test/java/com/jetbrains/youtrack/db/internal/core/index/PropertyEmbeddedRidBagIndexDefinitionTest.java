package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import org.junit.Assert;

/**
 * @since 1/30/14
 */
public class PropertyEmbeddedRidBagIndexDefinitionTest extends
    PropertyRidBagAbstractIndexDefinition {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD,
        Integer.MAX_VALUE);
    builder.addGlobalConfigurationParameter(
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD,
        Integer.MAX_VALUE);

    return builder.build();
  }

  @Override
  void assertEmbedded(RidBag ridBag) {
    Assert.assertTrue(ridBag.isEmbedded());
  }
}
