package com.jetbrains.youtrackdb.internal.core.gremlin.gremlintest;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

@Graph.OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
    method = "g_V_orXhasLabelXpersonX_hasXsoftware_name_lopXX_order_byXageX",
    reason = "The fork expectation uses the default order semantics.")
public class YTDBStandardOrderSemanticsGraphProvider extends YTDBGraphProvider {

  @Override
  public Map<String, Object> getBaseConfiguration(
      String graphName,
      Class<?> test,
      String testMethodName,
      LoadGraphWith.GraphData loadGraphWith) {
    var configuration = new HashMap<>(
        super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith));
    configuration.put(
        GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY.getKey(), Boolean.FALSE);
    return configuration;
  }
}
