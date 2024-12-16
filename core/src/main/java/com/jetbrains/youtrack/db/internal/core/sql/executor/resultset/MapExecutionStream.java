package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.query.Result;

public interface MapExecutionStream {

  ExecutionStream flatMap(Result next, CommandContext ctx);
}
