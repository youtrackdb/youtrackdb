package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLJson;

/**
 *
 */
public class UpdateMergeStep extends AbstractExecutionStep {

  private final SQLJson json;

  public UpdateMergeStep(SQLJson json, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;

    ExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private Result mapResult(Result result, CommandContext ctx) {
    if (result instanceof ResultInternal) {
      if (!(result.getEntity().orElse(null) instanceof EntityImpl)) {
        ((ResultInternal) result).setIdentifiable(result.toEntity().getRecord());
      }
      if (!(result.getEntity().orElse(null) instanceof EntityImpl)) {
        return result;
      }
      handleMerge((EntityImpl) result.getEntity().orElse(null), ctx);
    }
    return result;
  }

  private void handleMerge(EntityImpl record, CommandContext ctx) {
    record.merge(json.toDocument(record, ctx), true, false);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UPDATE MERGE\n" + spaces + "  " + json;
  }
}
