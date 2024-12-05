package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OIdentifier;
import com.orientechnologies.core.sql.parser.OStatement;
import com.orientechnologies.core.sql.parser.YTLocalResultSet;
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
      OCommandContext ctx,
      boolean profilingEnabled,
      List<String> scriptVars) {
    super(ctx, profilingEnabled);
    this.varName = varName;

    OBasicCommandContext subCtx = new OBasicCommandContext();
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
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    calculate(ctx);
    return OExecutionStream.empty();
  }

  private void calculate(OCommandContext ctx) {
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
