package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.query.Result;

public interface FilterResult {

  /**
   * Filter and change a result
   *
   * @param result to check
   * @param ctx    TODO
   * @return a new result or null if the current result need to be skipped
   */
  Result filterMap(Result result, CommandContext ctx);
}
