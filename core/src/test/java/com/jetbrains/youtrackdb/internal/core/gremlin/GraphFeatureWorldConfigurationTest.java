package com.jetbrains.youtrackdb.internal.core.gremlin;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.GraphFeatureWorld;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.YTDBGraphFeatureTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Guards feature-suite isolation and the intended scenario exclusions. */
@Category(SequentialTest.class)
public class GraphFeatureWorldConfigurationTest {

  @Test
  public void graphCacheKey_separatesOrderModesForOneTestClass() {
    var defaultMode = GraphFeatureWorld.cacheKey(YTDBGraphFeatureTest.class, false);
    var standardMode = GraphFeatureWorld.cacheKey(YTDBGraphFeatureTest.class, true);

    assertThat(defaultMode).isNotEqualTo(standardMode);
  }

  @Test
  public void ignoredScenarioCounts_matchBothFeatureExecutions() {
    assertThat(GraphFeatureWorld.ignoredScenarioCount(false)).isEqualTo(1);
    assertThat(GraphFeatureWorld.ignoredScenarioCount(true)).isEqualTo(7);
  }
}
