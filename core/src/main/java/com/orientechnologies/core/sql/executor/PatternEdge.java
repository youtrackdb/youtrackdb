package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.parser.OMatchPathItem;
import com.orientechnologies.core.sql.parser.OMatchStatement;

/**
 *
 */
public class PatternEdge {

  public PatternNode in;
  public PatternNode out;
  public OMatchPathItem item;

  public Iterable<YTIdentifiable> executeTraversal(
      OMatchStatement.MatchContext matchContext,
      OCommandContext iCommandContext,
      YTIdentifiable startingPoint,
      int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: " + in.alias + "}" + item.toString();
  }
}
