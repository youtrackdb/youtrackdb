package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OInsertExecutionPlan extends OSelectExecutionPlan {

  private final List<YTResult> result = new ArrayList<>();

  public OInsertExecutionPlan(CommandContext ctx) {
    super(ctx);
  }

  @Override
  public ExecutionStream start() {
    return ExecutionStream.resultIterator(result.iterator());
  }

  @Override
  public void reset(CommandContext ctx) {
    result.clear();
    super.reset(ctx);
    executeInternal();
  }

  public void executeInternal() throws YTCommandExecutionException {
    ExecutionStream nextBlock = super.start();

    while (nextBlock.hasNext(ctx)) {
      result.add(nextBlock.next(ctx));
    }
    nextBlock.close(ctx);
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal res = (YTResultInternal) super.toResult(db);
    res.setProperty("type", "InsertExecutionPlan");
    return res;
  }

  @Override
  public OInternalExecutionPlan copy(CommandContext ctx) {
    OInsertExecutionPlan copy = new OInsertExecutionPlan(ctx);
    super.copyOn(copy, ctx);
    return copy;
  }

  @Override
  public boolean canBeCached() {
    return super.canBeCached();
  }
}
