package com.jetbrains.youtrackdb.internal.server.gremlin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers explicit order modes through a remote GraphBinary client.
 * The server configuration is inspected because the client must inherit the deployment default.
 * The MODERN fixture supplies six vertices, including two software vertices without {@code age}.
 * Those fixture facts explain why retention returns six names and standard order returns four.
 */
public class StandardOrderSemanticsRemoteSuiteConfigTest {

  private YTDBGraphBinaryRemoteGraphProvider provider;

  @Before
  public void startServer() throws Exception {
    provider = new YTDBGraphBinaryRemoteGraphProvider();
    provider.startServer();
  }

  @After
  public void stopServer() {
    if (provider != null) {
      provider.stopServer();
      provider = null;
    }
  }

  /** Confirms that the server deployment default keeps records with a missing order key. */
  @Test
  public void remoteSuiteDatabase_usesExplicitDefaultMode() {
    try (var session = provider.ytdbServer.getYouTrackDB().open(
        "modern",
        YTDBGraphBinaryRemoteGraphProvider.ADMIN_USER_NAME,
        YTDBGraphBinaryRemoteGraphProvider.ADMIN_USER_PASSWORD)) {
      assertThat(session.getConfiguration().getValueAsBoolean(
          GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY)).isTrue();
    }
  }

  /** Confirms that explicit modes and the user strategy produce expected server-side results. */
  @Test
  public void remoteUserStrategy_reachesServerAndChangesTranslatedResult() throws Exception {
    try (var graph = provider.openTestGraph(provider.standardGraphConfiguration(
        getClass(), "remoteUserStrategy", GraphData.MODERN))) {
      var source = provider.traversal(graph);
      var defaultNames = source.with(YTDBQueryConfigParam.orderIncludesMissingKey, true)
          .V().order().by("age").values("name").toList();
      var settingNames = source.with(YTDBQueryConfigParam.orderIncludesMissingKey, false)
          .V().order().by("age").values("name").toList();
      var strategyNames = source.withStrategies(StandardOrderSemanticsStrategy.instance())
          .V().order().by("age").values("name").toList();

      assertThat(defaultNames).hasSize(6);
      assertThat(settingNames).containsExactly("vadas", "marko", "josh", "peter");
      assertThat(strategyNames).containsExactly("vadas", "marko", "josh", "peter");
    }
  }

  /**
   * Confirms that contradictory remote instructions fail instead of choosing one mode silently.
   */
  @Test
  public void remoteContradictoryInstructions_raiseError() throws Exception {
    try (var graph = provider.openTestGraph(provider.standardGraphConfiguration(
        getClass(), "remoteContradiction", GraphData.MODERN))) {
      var source = provider.traversal(graph);

      assertThatThrownBy(() -> source
          .with(YTDBQueryConfigParam.orderIncludesMissingKey, true)
          .withStrategies(StandardOrderSemanticsStrategy.instance())
          .V().order().by("age").values("name").toList())
          .hasMessageContaining("withoutStrategies(StandardOrderSemanticsStrategy.class)");
    }
  }
}
