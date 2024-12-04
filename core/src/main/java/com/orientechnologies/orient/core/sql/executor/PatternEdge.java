package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchStatement;

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
