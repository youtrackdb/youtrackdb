package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.gremlin.GraphBaseTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBTransaction;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.ContradictoryOrderSemanticsException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

/** Covers standard order semantics on the native execution path. */
public class YTDBStandardOrderSemanticsStrategyTest extends GraphBaseTest {

  @Test
  public void nativeDefault_keepsRecordWithoutOrderKey() {
    seedPeople();

    assertThat(nativeNames(() -> graph.traversal().V().hasLabel("Person")
        .order().by("age").values("name")))
        .containsExactlyInAnyOrder("Alice", "Bob", "Nobody")
        .containsSubsequence("Bob", "Alice");
  }

  @Test
  public void nativeSetting_selectsStandardOrderSemantics() {
    seedPeople();

    assertThat(withSetting(false, () -> nativeNames(() -> graph.traversal().V().hasLabel("Person")
        .order().by("age").values("name"))))
        .containsExactly("Bob", "Alice");
  }

  @Test
  public void nativeUserStrategy_selectsStandardOrderSemantics() {
    seedPeople();

    assertThat(nativeNames(() -> graph.traversal()
        .withStrategies(StandardOrderSemanticsStrategy.instance())
        .V().hasLabel("Person").order().by("age").values("name")))
        .containsExactly("Bob", "Alice");
  }

  @Test
  public void nativeContradictoryInstructions_raiseDedicatedError() {
    seedPeople();

    assertThatThrownBy(() -> nativeNames(() -> graph.traversal()
        .with(YTDBQueryConfigParam.orderIncludesMissingKey, true)
        .withStrategies(StandardOrderSemanticsStrategy.instance())
        .V().hasLabel("Person").order().by("age").values("name")))
        .isInstanceOf(ContradictoryOrderSemanticsException.class)
        .hasMessageContaining("withoutStrategies(StandardOrderSemanticsStrategy.class)");
  }

  @Test
  public void deploymentDefaultAndUserStrategy_areNotContradictory() {
    seedPeople();

    assertThat(withSetting(true, () -> nativeNames(() -> graph.traversal()
        .withStrategies(StandardOrderSemanticsStrategy.instance())
        .V().hasLabel("Person").order().by("age").values("name"))))
        .containsExactly("Bob", "Alice");
  }

  @Test
  public void nativeUserStrategy_reachesOrderInsideChildTraversal() {
    seedPeople();

    assertThat(nativeNames(() -> graph.traversal()
        .withStrategies(StandardOrderSemanticsStrategy.instance())
        .V().hasLabel("Person").union(__.order().by("age").values("name"))))
        .containsExactly("Bob", "Alice");
  }

  @Test
  public void nativeDefault_preservesForeignBypassAndMissingKeyRecord() {
    seedPeople();
    var keepEveryProperty = SubgraphStrategy.build()
        .vertexProperties(__.hasNot("unusedMetaProperty"))
        .create();

    assertThat(nativeNames(() -> graph.traversal().withStrategies(keepEveryProperty)
        .V().hasLabel("Person").order().by("age").values("name")))
        .containsExactlyInAnyOrder("Alice", "Bob", "Nobody");
  }

  @Test
  public void localOrder_keepsFilteringMissingKeys() {
    seedPeople();

    assertThat(withTranslator(false, () -> graph.traversal().V().hasLabel("Person")
        .values("age").fold().order(Scope.local).next()))
        .containsExactly(25, 30);
  }

  private void seedPeople() {
    graph.addVertex(T.label, "Person", "name", "Alice", "age", 30);
    graph.addVertex(T.label, "Person", "name", "Bob", "age", 25);
    graph.addVertex(T.label, "Person", "name", "Nobody");
    graph.tx().commit();
  }

  private List<String> nativeNames(Supplier<GraphTraversal<?, String>> traversal) {
    return withTranslator(false, () -> traversal.get().toList());
  }

  private <T> T withTranslator(boolean enabled, Supplier<T> body) {
    var configuration = graphConfiguration();
    var previous = configuration.getValueAsBoolean(
        GlobalConfiguration.QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED);
    configuration.setValue(GlobalConfiguration.QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED, enabled);
    try {
      return body.get();
    } finally {
      configuration.setValue(
          GlobalConfiguration.QUERY_GREMLIN_TO_MATCH_TRANSLATOR_ENABLED, previous);
    }
  }

  private <T> T withSetting(boolean includesMissingKey, Supplier<T> body) {
    var configuration = graphConfiguration();
    var previous = configuration.getValueAsBoolean(
        GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY);
    configuration.setValue(
        GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, includesMissingKey);
    try {
      return body.get();
    } finally {
      configuration.setValue(
          GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY, previous);
    }
  }

  private ContextConfiguration graphConfiguration() {
    var tx = (YTDBTransaction) graph.tx();
    tx.readWrite();
    return tx.getDatabaseSession().getConfiguration();
  }
}
