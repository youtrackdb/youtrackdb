package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class ReturnMatchElementsStep extends AbstractUnrollStep {

  public ReturnMatchElementsStep(CommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  protected Collection<YTResult> unroll(YTResult doc, CommandContext iContext) {
    List<YTResult> result = new ArrayList<>();
    for (String s : doc.getPropertyNames()) {
      if (!s.startsWith(OMatchExecutionPlanner.DEFAULT_ALIAS_PREFIX)) {
        Object elem = doc.getProperty(s);
        if (elem instanceof YTIdentifiable) {
          YTResultInternal newelem = new YTResultInternal(iContext.getDatabase(),
              (YTIdentifiable) elem);
          elem = newelem;
        }
        if (elem instanceof YTResult) {
          result.add((YTResult) elem);
        }
        // else...? TODO
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UNROLL $elements";
  }
}
