package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLWhereClause;
import java.util.List;

/**
 *
 */
public class UpsertStep extends AbstractExecutionStep {

  private final SQLFromClause commandTarget;
  private final SQLWhereClause initialFilter;

  public UpsertStep(
      SQLFromClause target, SQLWhereClause where, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    var prev = this.prev;
    assert prev != null;

    var upstream = prev.start(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream;
    }

    return ExecutionStream.singleton(createNewRecord(ctx, commandTarget, initialFilter));
  }

  private Result createNewRecord(
      CommandContext ctx, SQLFromClause commandTarget, SQLWhereClause initialFilter) {
    var db = ctx.getDatabase();
    EntityImpl entity;
    if (commandTarget.getItem().getIdentifier() != null) {
      entity = new EntityImpl(db, commandTarget.getItem().getIdentifier().getStringValue());
    } else if (commandTarget.getItem().getCluster() != null) {
      var cluster = commandTarget.getItem().getCluster();
      var clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = ctx.getDatabase().getClusterIdByName(cluster.getClusterName());
      }
      var clazz =
          ctx.getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClassByClusterId(clusterId);
      entity = new EntityImpl(db, clazz);
    } else {
      throw new CommandExecutionException(
          "Cannot execute UPSERT on target '" + commandTarget + "'");
    }

    var result = new UpdatableResult(ctx.getDatabase(), entity);
    if (initialFilter != null) {
      setContent(result, initialFilter);
    }
    return result;
  }

  private void setContent(ResultInternal res, SQLWhereClause initialFilter) {
    var flattened = initialFilter.flatten();
    if (flattened.isEmpty()) {
      return;
    }
    if (flattened.size() > 1) {
      throw new CommandExecutionException("Cannot UPSERT on OR conditions");
    }
    var andCond = flattened.get(0);
    for (var condition : andCond.getSubBlocks()) {
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(res, ctx));
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
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
