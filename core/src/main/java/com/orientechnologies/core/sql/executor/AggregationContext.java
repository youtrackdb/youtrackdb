package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;

/**
 *
 */
public interface AggregationContext {

  Object getFinalValue();

  void apply(YTResult next, OCommandContext ctx);
}
