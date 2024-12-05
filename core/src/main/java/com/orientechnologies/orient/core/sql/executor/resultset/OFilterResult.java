package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.YTResult;

public interface OFilterResult {

  /**
   * Filter and change a result
   *
   * @param result to check
   * @param ctx    TODO
   * @return a new result or null if the current result need to be skipped
   */
  YTResult filterMap(YTResult result, OCommandContext ctx);
}
