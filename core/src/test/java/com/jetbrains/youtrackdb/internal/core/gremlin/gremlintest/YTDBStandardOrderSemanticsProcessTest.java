package com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest;

import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBGraph;
import com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest.suites.YTDBProcessSuiteEmbedded;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/** Runs process compliance with standard order semantics. */
@Category(SequentialTest.class)
@RunWith(YTDBProcessSuiteEmbedded.class)
@GraphProviderClass(provider = YTDBStandardOrderSemanticsGraphProvider.class,
    graph = YTDBGraph.class)
public class YTDBStandardOrderSemanticsProcessTest {
}
