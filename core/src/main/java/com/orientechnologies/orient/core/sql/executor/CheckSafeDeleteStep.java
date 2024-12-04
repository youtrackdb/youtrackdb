package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * Checks if a record can be safely deleted (throws YTCommandExecutionException in case). A record
 * cannot be safely deleted if it's a vertex or an edge (it requires additional operations).
 *
 * <p>The result set returned by syncPull() throws an YTCommandExecutionException as soon as it
 * finds a record that cannot be safely deleted (eg. a vertex or an edge)
 *
 * <p>This step is used used in DELETE statement to make sure that you are not deleting vertices or
 * edges without passing for an explicit DELETE VERTEX/EDGE
 */
public class CheckSafeDeleteStep extends AbstractExecutionStep {

  public CheckSafeDeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result.isElement()) {
      var elem = result.toElement();
      YTClass clazz = ODocumentInternal.getImmutableSchemaClass((YTDocument) elem);
      if (clazz != null) {
        if (clazz.getName().equalsIgnoreCase("V") || clazz.isSubClassOf("V")) {
          throw new YTCommandExecutionException(
              "Cannot safely delete a vertex, please use DELETE VERTEX or UNSAFE");
        }
        if (clazz.getName().equalsIgnoreCase("E") || clazz.isSubClassOf("E")) {
          throw new YTCommandExecutionException(
              "Cannot safely delete an edge, please use DELETE EDGE or UNSAFE");
        }
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK SAFE DELETE");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }
}
