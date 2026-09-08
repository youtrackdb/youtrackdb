package com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest;

import com.jetbrains.youtrackdb.internal.SequentialTest;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/** Runs feature compliance with standard order semantics. */
@Category(SequentialTest.class)
@RunWith(Cucumber.class)
@CucumberOptions(
    tags = "not @RemoteOnly and not @MultiProperties and not @GraphComputerOnly "
        + "and not @UserSuppliedVertexPropertyIds and not @UserSuppliedEdgeIds "
        + "and not @UserSuppliedVertexIds and not @TinkerServiceRegistry "
        + "and not @DisallowNullPropertyValues and not @InsertionOrderingRequired "
        + "and not @DataUUID and not @DataDateTime",
    glue = {"org.apache.tinkerpop.gremlin.features"},
    objectFactory = GuiceFactory.class,
    features = {"classpath:/org/apache/tinkerpop/gremlin/test/features",
        "classpath:/com/jetbrains/youtrackdb/internal/core/gremlin/gremlintest/features"},
    plugin = {"progress", "junit:target/cucumber-standard-order-semantics.xml"})
public class YTDBStandardOrderSemanticsFeatureTest {
}
