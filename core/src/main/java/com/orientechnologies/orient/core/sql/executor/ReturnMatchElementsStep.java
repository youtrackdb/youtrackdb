package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class ReturnMatchElementsStep extends AbstractUnrollStep {

  public ReturnMatchElementsStep(OCommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  protected Collection<YTResult> unroll(YTResult doc, OCommandContext iContext) {
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UNROLL $elements";
  }
}
