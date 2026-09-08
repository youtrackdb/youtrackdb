package com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.scenarios;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Verifies global order modes with the TinkerPop {@link GraphData#MODERN} fixture.
 * The fixture contains six vertices, including software vertices that have no {@code age} key.
 * Each test sets its order mode explicitly so the expected result does not depend on suite state.
 */
@Category(SequentialTest.class)
@RunWith(GremlinProcessRunner.class)
public class YTDBProductiveOrderOptOutProcessTest extends YTDBAbstractGremlinTest {

  /**
   * The retention mode keeps all six fixture vertices.
   * Both software vertices lack {@code age}.
   */
  @Test
  @LoadGraphWith(GraphData.MODERN)
  public void explicitDefaultMode_keepsEveryVertex() {
    var ordered = g().with(YTDBQueryConfigParam.orderIncludesMissingKey, true)
        .V().order().by("age").values("name").toList();

    assertThat(ordered).hasSize(6);
  }

  /** The standard-order mode removes the two software vertices that lack {@code age}. */
  @Test
  @LoadGraphWith(GraphData.MODERN)
  public void explicitStandardOrderSemantics_dropVerticesWithoutAge() {
    var ordered = g().with(YTDBQueryConfigParam.orderIncludesMissingKey, false)
        .V().order().by("age").values("name").toList();

    assertThat(ordered).containsExactly("vadas", "marko", "josh", "peter");
  }

  /** The explicit retention option and standard-order strategy produce a contradiction error. */
  @Test
  @LoadGraphWith(GraphData.MODERN)
  public void explicitContradiction_raisesDedicatedError() {
    assertThatThrownBy(() -> g().with(YTDBQueryConfigParam.orderIncludesMissingKey, true)
        .withStrategies(StandardOrderSemanticsStrategy.instance())
        .V().order().by("age").values("name").toList())
        .hasMessageContaining("withoutStrategies(StandardOrderSemanticsStrategy.class)");
  }
}
