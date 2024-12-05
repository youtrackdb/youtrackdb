package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ODDLStatement;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ODDLExecutionPlan implements OInternalExecutionPlan {

  private final ODDLStatement statement;
  private final CommandContext ctx;

  private boolean executed = false;

  public ODDLExecutionPlan(CommandContext ctx, ODDLStatement stm) {
    this.ctx = ctx;
    this.statement = stm;
  }

  @Override
  public void close() {
  }

  @Override
  public CommandContext getContext() {
    return ctx;
  }

  @Override
  public ExecutionStream start() {
    return ExecutionStream.empty();
  }

  public void reset(CommandContext ctx) {
    executed = false;
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public ExecutionStream executeInternal(BasicCommandContext ctx)
      throws YTCommandExecutionException {
    if (executed) {
      throw new YTCommandExecutionException(
          "Trying to execute a result-set twice. Please use reset()");
    }
    executed = true;
    ExecutionStream result = statement.executeDDL(this.ctx);
    return result;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ DDL\n" + "  " + statement.toString();
    return result;
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("type", "DDLExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("stmText", statement.toString());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    return result;
  }
}
