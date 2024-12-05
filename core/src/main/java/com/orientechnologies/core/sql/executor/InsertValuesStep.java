package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.OResultMapper;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OIdentifier;
import com.orientechnologies.core.sql.parser.OUpdateItem;
import java.util.List;

/**
 *
 */
public class InsertValuesStep extends AbstractExecutionStep {

  private final List<OIdentifier> identifiers;
  private final List<List<OExpression>> values;

  public InsertValuesStep(
      List<OIdentifier> identifierList,
      List<List<OExpression>> valueExpressions,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.identifiers = identifierList;
    this.values = valueExpressions;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);

    return upstream.map(
        new OResultMapper() {

          private int nextValueSet = 0;

          @Override
          public YTResult map(YTResult result, OCommandContext ctx) {
            if (!(result instanceof YTResultInternal)) {
              if (!result.isEntity()) {
                throw new YTCommandExecutionException(
                    "Error executing INSERT, cannot modify element: " + result);
              }
              result = new YTUpdatableResult(ctx.getDatabase(), result.toEntity());
            }
            List<OExpression> currentValues = values.get(nextValueSet++);
            if (currentValues.size() != identifiers.size()) {
              throw new YTCommandExecutionException(
                  "Cannot execute INSERT, the number of fields is different from the number of"
                      + " expressions: "
                      + identifiers
                      + " "
                      + currentValues);
            }
            nextValueSet %= values.size();
            for (int i = 0; i < currentValues.size(); i++) {
              OIdentifier identifier = identifiers.get(i);
              Object value = currentValues.get(i).execute(result, ctx);
              value =
                  OUpdateItem.convertToPropertyType(
                      (YTResultInternal) result, identifier, value, ctx);
              ((YTResultInternal) result).setProperty(identifier.getStringValue(), value);
            }
            return result;
          }
        });
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET VALUES \n");
    result.append(spaces);
    result.append("  (");
    for (int i = 0; i < identifiers.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      result.append(identifiers.get(i));
    }
    result.append(")\n");

    result.append(spaces);
    result.append("  VALUES\n");

    for (int c = 0; c < this.values.size(); c++) {
      if (c > 0) {
        result.append("\n");
      }
      List<OExpression> exprs = this.values.get(c);
      result.append(spaces);
      result.append("  (");
      for (int i = 0; i < exprs.size() && i < 3; i++) {
        if (i > 0) {
          result.append(", ");
        }
        result.append(exprs.get(i));
      }
      result.append(")");
    }
    if (this.values.size() >= 3) {
      result.append(spaces);
      result.append("  ...");
    }
    return result.toString();
  }
}
