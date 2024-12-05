package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OForEachBlock;
import com.orientechnologies.core.sql.parser.OIdentifier;
import com.orientechnologies.core.sql.parser.OIfStatement;
import com.orientechnologies.core.sql.parser.OReturnStatement;
import com.orientechnologies.core.sql.parser.OStatement;
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
      OCommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.loopVariable = loopVariable;
    this.source = oExpression;
    this.body = statements;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;

    OExecutionStream prevStream = prev.start(ctx);
    prevStream.close(ctx);
    Iterator<?> iterator = init(ctx);
    while (iterator.hasNext()) {
      ctx.setVariable(loopVariable.getStringValue(), iterator.next());
      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        return result.start(ctx);
      }
    }

    return new EmptyStep(ctx, false).start(ctx);
  }

  protected Iterator<?> init(OCommandContext ctx) {
    var db = ctx.getDatabase();
    Object val = source.execute(new YTResultInternal(db), ctx);
    return OMultiValue.getMultiValueIterator(val);
  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
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
