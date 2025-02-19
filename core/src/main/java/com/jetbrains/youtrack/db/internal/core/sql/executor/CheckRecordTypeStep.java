package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;

/**
 * Checks that all the records from the upstream are of a particular type (or subclasses). Throws
 * CommandExecutionException in case it's not true
 */
public class CheckRecordTypeStep extends AbstractExecutionStep {

  private final String clazz;

  public CheckRecordTypeStep(CommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clazz = className;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;
    var upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private Result mapResult(Result result, CommandContext ctx) {
    if (!result.isEntity()) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "record " + result + " is not an instance of " + clazz);
    }
    var entity = (EntityInternal) result.asEntity();
    if (entity == null) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "record " + result + " is not an instance of " + clazz);
    }
    var session = ctx.getDatabaseSession();
    var schema = entity.getImmutableSchemaClass(session);

    if (schema == null || !schema.isSubClassOf(session, clazz)) {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "record " + result + " is not an instance of " + clazz);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var result = ExecutionStepInternal.getIndent(depth, indent) + "+ CHECK RECORD TYPE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (ExecutionStepInternal.getIndent(depth, indent) + "  " + clazz);
    return result;
  }
}
