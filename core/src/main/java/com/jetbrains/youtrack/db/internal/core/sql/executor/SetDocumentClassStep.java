package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;

/**
 * Assigns a class to documents coming from upstream
 */
public class SetDocumentClassStep extends AbstractExecutionStep {

  private final String targetClass;

  public SetDocumentClassStep(
      SQLIdentifier targetClass, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass.getStringValue();
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;

    ExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private Result mapResult(Result result, CommandContext ctx) {
    if (result.isEntity()) {
      var element = result.toEntity();
      ((EntityImpl) element).setClassName(targetClass);
      if (!(result instanceof ResultInternal)) {
        result = new UpdatableResult(ctx.getDatabase(), element);
      } else {
        ((ResultInternal) result).setIdentifiable(element);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ SET CLASS\n" + spaces + "  " + this.targetClass;
  }
}
