package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
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
      OCommandContext ctx,
      OWhereClause condition,
      OInternalExecutionPlan body,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.body = body;
    this.condition = condition;
  }

  @Override
  protected Collection<OResult> unroll(OResult doc, OCommandContext iContext) {
    body.reset(iContext);
    List<OResult> result = new ArrayList<>();
    OExecutionStream block = body.start();
    while (block.hasNext(iContext)) {
      result.add(block.next(iContext));
    }
    block.close(iContext);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String indentStep = OExecutionStepInternal.getIndent(1, indent);
    String spaces = OExecutionStepInternal.getIndent(depth, indent);

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
