package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OWhereClause;
import java.util.List;

/**
 *
 */
public class UpsertStep extends AbstractExecutionStep {

  private final OFromClause commandTarget;
  private final OWhereClause initialFilter;

  public UpsertStep(
      OFromClause target, OWhereClause where, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    var prev = this.prev;
    assert prev != null;

    ExecutionStream upstream = prev.start(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream;
    }

    return ExecutionStream.singleton(createNewRecord(ctx, commandTarget, initialFilter));
  }

  private YTResult createNewRecord(
      CommandContext ctx, OFromClause commandTarget, OWhereClause initialFilter) {
    EntityImpl doc;
    if (commandTarget.getItem().getIdentifier() != null) {
      doc = new EntityImpl(commandTarget.getItem().getIdentifier().getStringValue());
    } else if (commandTarget.getItem().getCluster() != null) {
      OCluster cluster = commandTarget.getItem().getCluster();
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = ctx.getDatabase().getClusterIdByName(cluster.getClusterName());
      }
      YTClass clazz =
          ctx.getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClassByClusterId(clusterId);
      doc = new EntityImpl(clazz);
    } else {
      throw new YTCommandExecutionException(
          "Cannot execute UPSERT on target '" + commandTarget + "'");
    }

    YTUpdatableResult result = new YTUpdatableResult(ctx.getDatabase(), doc);
    if (initialFilter != null) {
      setContent(result, initialFilter);
    }
    return result;
  }

  private void setContent(YTResultInternal doc, OWhereClause initialFilter) {
    List<OAndBlock> flattened = initialFilter.flatten();
    if (flattened.isEmpty()) {
      return;
    }
    if (flattened.size() > 1) {
      throw new YTCommandExecutionException("Cannot UPSERT on OR conditions");
    }
    OAndBlock andCond = flattened.get(0);
    for (OBooleanExpression condition : andCond.getSubBlocks()) {
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(doc, ctx));
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces
        + "+ INSERT (upsert, if needed)\n"
        + spaces
        + "  target: "
        + commandTarget
        + "\n"
        + spaces
        + "  content: "
        + initialFilter;
  }
}
