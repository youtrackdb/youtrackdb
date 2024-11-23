package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 *
 */
public interface AggregationContext {

  Object getFinalValue();

  void apply(OResult next, OCommandContext ctx);
}
