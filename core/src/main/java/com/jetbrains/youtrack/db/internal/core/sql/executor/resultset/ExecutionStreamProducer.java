package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;

public interface ExecutionStreamProducer {

  boolean hasNext(CommandContext ctx);

  ExecutionStream next(CommandContext ctx);

  void close(CommandContext ctx);
}
