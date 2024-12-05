package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ResultSetEdgeTraverser;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFieldMatchPathItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLMultiMatchPathItem;

/**
 *
 */
public class MatchStep extends AbstractExecutionStep {

  protected final EdgeTraversal edge;

  public MatchStep(CommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.edge = edge;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    ExecutionStream resultSet = prev.start(ctx);
    return resultSet.flatMap(this::createNextResultSet);
  }

  public ExecutionStream createNextResultSet(YTResult lastUpstreamRecord, CommandContext ctx) {
    MatchEdgeTraverser trav = createTraverser(lastUpstreamRecord);
    return new ResultSetEdgeTraverser(trav);
  }

  protected MatchEdgeTraverser createTraverser(YTResult lastUpstreamRecord) {
    if (edge.edge.item instanceof SQLMultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.edge.item instanceof SQLFieldMatchPathItem) {
      return new MatchFieldTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
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
    if (edge.edge.item instanceof SQLFieldMatchPathItem) {
      result.append(".");
      result.append(((SQLFieldMatchPathItem) edge.edge.item).getField());
    } else {
      result.append(edge.edge.item.getMethod());
    }
    result.append("{").append(edge.edge.in.alias).append("}");
    return result.toString();
  }
}
