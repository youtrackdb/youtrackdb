package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class WhileMatchStep extends AbstractUnrollStep {

  private final OInternalExecutionPlan body;
  private final OWhereClause condition;

  public WhileMatchStep(
      CommandContext ctx,
      OWhereClause condition,
      OInternalExecutionPlan body,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.body = body;
    this.condition = condition;
  }

  @Override
  protected Collection<YTResult> unroll(YTResult doc, CommandContext iContext) {
    body.reset(iContext);
    List<YTResult> result = new ArrayList<>();
    ExecutionStream block = body.start();
    while (block.hasNext(iContext)) {
      result.add(block.next(iContext));
    }
    block.close(iContext);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String indentStep = ExecutionStepInternal.getIndent(1, indent);
    String spaces = ExecutionStepInternal.getIndent(depth, indent);

    String result =
        spaces
            + "+ WHILE\n"
            + spaces
            + indentStep
            + condition.toString()
            + "\n"
            + spaces
            + "  DO\n"
            + body.prettyPrint(depth + 1, indent)
            + "\n"
            + spaces
            + "  END\n";

    return result;
  }
}
