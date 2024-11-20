package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 16/07/16.
 */
public interface AggregationContext {

  Object getFinalValue();

  void apply(OResult next, OCommandContext ctx);
}
