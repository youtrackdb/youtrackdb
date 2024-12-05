package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;

/**
 *
 */
public interface AggregationContext {

  Object getFinalValue();

  void apply(YTResult next, OCommandContext ctx);
}
