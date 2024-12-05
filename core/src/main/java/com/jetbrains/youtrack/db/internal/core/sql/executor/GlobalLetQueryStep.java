package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.YTLocalResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class GlobalLetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OInternalExecutionPlan subExecutionPlan;

  public GlobalLetQueryStep(
      OIdentifier varName,
      OStatement query,
      CommandContext ctx,
      boolean profilingEnabled,
      List<String> scriptVars) {
    super(ctx, profilingEnabled);
    this.varName = varName;

    BasicCommandContext subCtx = new BasicCommandContext();
    if (scriptVars != null) {
      scriptVars.forEach(subCtx::declareScriptVariable);
    }
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    if (query.toString().contains("?")) {
      // with positional parameters, you cannot know if a parameter has the same ordinal as the one
      // cached
      subExecutionPlan = query.createExecutionPlanNoCache(subCtx, profilingEnabled);
    } else {
      subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
    }
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    calculate(ctx);
    return ExecutionStream.empty();
  }

  private void calculate(CommandContext ctx) {
    ctx.setVariable(varName.getStringValue(), toList(new YTLocalResultSet(subExecutionPlan)));
  }

  private List<YTResult> toList(YTLocalResultSet oLocalResultSet) {
    List<YTResult> result = new ArrayList<>();
    while (oLocalResultSet.hasNext()) {
      result.add(oLocalResultSet.next());
    }
    oLocalResultSet.close();
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces
        + "+ LET (once)\n"
        + spaces
        + "  "
        + varName
        + " = \n"
        + box(spaces + "    ", this.subExecutionPlan.prettyPrint(0, indent));
  }

  @Override
  public List<OExecutionPlan> getSubExecutionPlans() {
    return Collections.singletonList(this.subExecutionPlan);
  }

  private String box(String spaces, String s) {
    String[] rows = s.split("\n");
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+-------------------------\n");
    for (String row : rows) {
      result.append(spaces);
      result.append("| ");
      result.append(row);
      result.append("\n");
    }
    result.append(spaces);
    result.append("+-------------------------");
    return result.toString();
  }
}
