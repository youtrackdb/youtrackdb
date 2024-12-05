package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OForEachBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIfStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OReturnStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatement;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ForEachStep extends AbstractExecutionStep {

  private final OIdentifier loopVariable;
  private final OExpression source;
  public List<OStatement> body;

  public ForEachStep(
      OIdentifier loopVariable,
      OExpression oExpression,
      List<OStatement> statements,
      CommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.loopVariable = loopVariable;
    this.source = oExpression;
    this.body = statements;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    ExecutionStream prevStream = prev.start(ctx);
    prevStream.close(ctx);
    Iterator<?> iterator = init(ctx);
    while (iterator.hasNext()) {
      ctx.setVariable(loopVariable.getStringValue(), iterator.next());
      OScriptExecutionPlan plan = initPlan(ctx);
      ExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        return result.start(ctx);
      }
    }

    return new EmptyStep(ctx, false).start(ctx);
  }

  protected Iterator<?> init(CommandContext ctx) {
    var db = ctx.getDatabase();
    Object val = source.execute(new YTResultInternal(db), ctx);
    return OMultiValue.getMultiValueIterator(val);
  }

  public OScriptExecutionPlan initPlan(CommandContext ctx) {
    BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : body) {
      plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return plan;
  }

  public boolean containsReturn() {
    for (OStatement stm : this.body) {
      if (stm instanceof OReturnStatement) {
        return true;
      }
      if (stm instanceof OForEachBlock && ((OForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OIfStatement && ((OIfStatement) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
