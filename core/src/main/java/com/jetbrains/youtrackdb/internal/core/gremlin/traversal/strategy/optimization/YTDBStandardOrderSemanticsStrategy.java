package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.GremlinToMatchStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;

/**
 * Applies standard order semantics to native Gremlin execution when the effective mode requests
 * record removal. The translated path applies the same mode through its order-key presence policy.
 */
public final class YTDBStandardOrderSemanticsStrategy
    extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
    implements ProviderOptimizationStrategy {

  private static final YTDBStandardOrderSemanticsStrategy INSTANCE =
      new YTDBStandardOrderSemanticsStrategy();

  private YTDBStandardOrderSemanticsStrategy() {
  }

  public static YTDBStandardOrderSemanticsStrategy instance() {
    return INSTANCE;
  }

  /**
   * Runs after translation. A translated traversal has no order step. A declined traversal keeps
   * the native order step and receives standard order semantics here.
   */
  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return Set.of(GremlinToMatchStrategy.class);
  }

  @Override
  public void apply(Admin<?, ?> traversal) {
    if (YTDBStrategyUtil.orderIncludesMissingKey(traversal)
        || YTDBStrategyUtil.hasStandardOrderSemanticsStrategy(traversal)) {
      return;
    }
    StandardOrderSemanticsStrategy.instance().apply(traversal);
  }
}
