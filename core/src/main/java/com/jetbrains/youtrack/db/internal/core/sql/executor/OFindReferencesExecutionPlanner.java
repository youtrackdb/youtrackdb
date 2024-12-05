package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFindReferencesStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLRid;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SimpleNode;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class OFindReferencesExecutionPlanner {

  protected SQLRid rid;
  protected SQLStatement subQuery;

  // class or cluster
  protected List<SimpleNode> targets;

  public OFindReferencesExecutionPlanner(SQLFindReferencesStatement statement) {
    // copying the content, so that it can be manipulated and optimized
    this.rid = statement.getRid() == null ? null : statement.getRid().copy();
    this.subQuery = statement.getSubQuery() == null ? null : statement.getSubQuery().copy();
    this.targets =
        statement.getTargets() == null
            ? null
            : statement.getTargets().stream().map(x -> x.copy()).collect(Collectors.toList());
  }

  public OInternalExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    handleRidSource(plan, ctx, enableProfiling);
    handleSubQuerySource(plan, ctx, enableProfiling);
    handleFindReferences(plan, ctx, enableProfiling);
    return plan;
  }

  private void handleFindReferences(
      OSelectExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    List<SQLIdentifier> classes = null;
    List<SQLCluster> clusters = null;
    if (targets != null) {
      classes =
          targets.stream()
              .filter(x -> x instanceof SQLIdentifier)
              .map(y -> ((SQLIdentifier) y))
              .collect(Collectors.toList());
      clusters =
          targets.stream()
              .filter(x -> x instanceof SQLCluster)
              .map(y -> ((SQLCluster) y))
              .collect(Collectors.toList());
    }

    plan.chain(new FindReferencesStep(classes, clusters, ctx, profilingEnabled));
  }

  private void handleSubQuerySource(
      OSelectExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    if (subQuery != null) {
      plan.chain(
          new SubQueryStep(
              subQuery.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
    }
  }

  private void handleRidSource(
      OSelectExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    if (rid != null) {
      plan.chain(
          new FetchFromRidsStep(
              Collections.singleton(rid.toRecordId((YTResult) null, ctx)), ctx, profilingEnabled));
    }
  }
}
