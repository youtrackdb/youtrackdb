package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OResultSetEdgeTraverser;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFieldMatchPathItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OMultiMatchPathItem;

/**
 *
 */
public class MatchStep extends AbstractExecutionStep {

  protected final EdgeTraversal edge;

  public MatchStep(OCommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.edge = edge;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(this::createNextResultSet);
  }

  public OExecutionStream createNextResultSet(YTResult lastUpstreamRecord, OCommandContext ctx) {
    MatchEdgeTraverser trav = createTraverser(lastUpstreamRecord);
    return new OResultSetEdgeTraverser(trav);
  }

  protected MatchEdgeTraverser createTraverser(YTResult lastUpstreamRecord) {
    if (edge.edge.item instanceof OMultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.edge.item instanceof OFieldMatchPathItem) {
      return new MatchFieldTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MATCH ");
    if (edge.out) {
      result.append("     ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{").append(edge.edge.out.alias).append("}");
    if (edge.edge.item instanceof OFieldMatchPathItem) {
      result.append(".");
      result.append(((OFieldMatchPathItem) edge.edge.item).getField());
    } else {
      result.append(edge.edge.item.getMethod());
    }
    result.append("{").append(edge.edge.in.alias).append("}");
    return result.toString();
  }
}
