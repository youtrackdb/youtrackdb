package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;

public interface OExecutionStreamProducer {

  boolean hasNext(OCommandContext ctx);

  OExecutionStream next(OCommandContext ctx);

  void close(OCommandContext ctx);
}
