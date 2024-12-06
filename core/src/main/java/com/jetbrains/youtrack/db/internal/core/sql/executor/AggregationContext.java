package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;

/**
 *
 */
public interface AggregationContext {

  Object getFinalValue();

  void apply(Result next, CommandContext ctx);
}
