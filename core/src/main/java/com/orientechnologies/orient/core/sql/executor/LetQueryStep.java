package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.YTLocalResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class LetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OStatement query;

  public LetQueryStep(
      OIdentifier varName, OStatement query, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varName = varName;
    this.query = query;
  }

  private YTResultInternal calculate(YTResultInternal result, OCommandContext ctx) {
    OBasicCommandContext subCtx = new OBasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParentWithoutOverridingChild(ctx);
    OInternalExecutionPlan subExecutionPlan;
    if (query.toString().contains("?")) {
      // with positional parameters, you cannot know if a parameter has the same ordinal as the
      // one cached
      subExecutionPlan = query.createExecutionPlanNoCache(subCtx, profilingEnabled);
    } else {
      subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
    }
    result.setMetadata(varName.getStringValue(), toList(new YTLocalResultSet(subExecutionPlan)));
    return result;
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
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException(
          "Cannot execute a local LET on a query without a target");
    }
    return prev.start(ctx).map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    return calculate((YTResultInternal) result, ctx);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = (" + query + ")";
  }
}
