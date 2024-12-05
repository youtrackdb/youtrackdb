package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;

public interface OExecutionStreamProducer {

  boolean hasNext(OCommandContext ctx);

  OExecutionStream next(OCommandContext ctx);

  void close(OCommandContext ctx);
}
