package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLMatchPathItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLMatchStatement;

/**
 *
 */
public class PatternEdge {

  public PatternNode in;
  public PatternNode out;
  public SQLMatchPathItem item;

  public Iterable<YTIdentifiable> executeTraversal(
      SQLMatchStatement.MatchContext matchContext,
      CommandContext iCommandContext,
      YTIdentifiable startingPoint,
      int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: " + in.alias + "}" + item.toString();
  }
}
