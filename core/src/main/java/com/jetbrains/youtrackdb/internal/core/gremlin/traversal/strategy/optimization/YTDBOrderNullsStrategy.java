package com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization;

import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.GremlinToMatchStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.YTDBStrategyUtil;
import com.jetbrains.youtrackdb.internal.core.sql.OrderByNullsUtil;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;

/**
 * Applies the direction-specific null placement settings to native Gremlin {@code order()} steps.
 * TinkerPop's default comparators already place nulls first for ascending order and last for
 * descending order. The strategy rebuilds each {@link OrderGlobalStep} /
 * {@link OrderLocalStep} with wrapped framework {@link Order#asc} / {@link Order#desc}
 * comparators so null placement matches YQL {@code ORDER BY}.
 *
 * <p>Runs after {@link GremlinToMatchStrategy} and {@link YTDBStandardOrderSemanticsStrategy}. A
 * recognized shape loses its order step to the MATCH splice. The standard-order strategy must
 * decide missing-key behavior before comparator wrapping.
 *
 * <p>Only the two framework order constants are wrapped. {@link Order#shuffle} and caller-supplied
 * comparators keep their own null handling. The strategy does not walk nested traversals: the
 * framework already visits each child once.
 */
public final class YTDBOrderNullsStrategy
    extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
    implements ProviderOptimizationStrategy {

  private static final YTDBOrderNullsStrategy INSTANCE = new YTDBOrderNullsStrategy();

  private YTDBOrderNullsStrategy() {
  }

  public static YTDBOrderNullsStrategy instance() {
    return INSTANCE;
  }

  @Override
  public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
    return Set.of(GremlinToMatchStrategy.class, YTDBStandardOrderSemanticsStrategy.class);
  }

  @Override
  public void apply(Admin<?, ?> traversal) {
    var session = YTDBStrategyUtil.resolveYtdbSession(traversal);
    if (session == null) {
      return;
    }
    var config = session.getConfiguration();
    if (config == null) {
      return;
    }
    // One read for the whole apply. Every wrap below reuses this value.
    var placements = OrderByNullsUtil.resolvePlacements(config);

    for (OrderGlobalStep<?, ?> step : TraversalHelper.getStepsOfAssignableClass(
        OrderGlobalStep.class, traversal)) {
      rebuildGlobal(step, traversal, placements);
    }
    for (OrderLocalStep<?, ?> step : TraversalHelper.getStepsOfAssignableClass(
        OrderLocalStep.class, traversal)) {
      rebuildLocal(step, traversal, placements);
    }
  }

  /**
   * Rebuilds a global order step when it holds a framework {@code asc}/{@code desc} comparator.
   * {@code getComparators()} is unmodifiable (and synthesizes identity+asc for a bare {@code
   * order()}), so the pairs are copied onto a fresh step and the original is replaced.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void rebuildGlobal(
      OrderGlobalStep<?, ?> step,
      Admin<?, ?> traversal,
      ResolvedOrderByNullsPlacement placements) {
    var pairs = (List<Pair<Admin, Comparator>>) (List<?>) step.getComparators();
    if (!needsWrap(pairs, placements)) {
      return;
    }
    var replacement = new OrderGlobalStep<>(traversal);
    replacement.setLimit(step.getLimit());
    if (step.isFilteringUnproductiveTraversers()) {
      replacement.enableFilteringUnproductiveTraversers();
    }
    step.getLabels().forEach(replacement::addLabel);
    for (var pair : pairs) {
      replacement.addComparator(pair.getValue0(), maybeWrap(pair.getValue1(), placements));
    }
    TraversalHelper.replaceStep((Step) step, replacement, traversal);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void rebuildLocal(
      OrderLocalStep<?, ?> step,
      Admin<?, ?> traversal,
      ResolvedOrderByNullsPlacement placements) {
    var pairs = (List<Pair<Admin, Comparator>>) (List<?>) step.getComparators();
    if (!needsWrap(pairs, placements)) {
      return;
    }
    var replacement = new OrderLocalStep<>(traversal);
    if (step.isFilteringUnproductiveTraversers()) {
      replacement.enableFilteringUnproductiveTraversers();
    }
    step.getLabels().forEach(replacement::addLabel);
    for (var pair : pairs) {
      replacement.addComparator(pair.getValue0(), maybeWrap(pair.getValue1(), placements));
    }
    TraversalHelper.replaceStep((Step) step, replacement, traversal);
  }

  @SuppressWarnings("rawtypes")
  private static boolean needsWrap(
      List<Pair<Admin, Comparator>> pairs, ResolvedOrderByNullsPlacement placements) {
    for (var pair : pairs) {
      var comparator = pair.getValue1();
      if ((comparator == Order.asc && placements.ascending() == OrderByNullsPlacement.LAST)
          || (comparator == Order.desc
              && placements.descending() == OrderByNullsPlacement.FIRST)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Comparator maybeWrap(
      Comparator comparator, ResolvedOrderByNullsPlacement placements) {
    if (comparator == Order.asc && placements.ascending() == OrderByNullsPlacement.LAST) {
      return wrap(comparator, false);
    }
    if (comparator == Order.desc && placements.descending() == OrderByNullsPlacement.FIRST) {
      return wrap(comparator, true);
    }
    // shuffle and caller-supplied comparators keep their own null handling.
    return comparator;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Comparator wrap(Comparator delegate, boolean nullsFirst) {
    return (a, b) -> {
      if (a == null || b == null) {
        if (a == null && b == null) {
          return 0;
        }
        if (a == null) {
          return nullsFirst ? -1 : 1;
        }
        return nullsFirst ? 1 : -1;
      }
      return delegate.compare(a, b);
    };
  }
}
