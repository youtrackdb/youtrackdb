package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTVertexInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Collection;

/**
 * after an update of an edge, this step updates edge pointers on vertices to make the graph
 * consistent again
 */
public class UpdateEdgePointersStep extends AbstractExecutionStep {

  public UpdateEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    var prev = this.prev;
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, OCommandContext ctx) {
    if (result instanceof YTResultInternal) {
      handleUpdateEdge(result.toEntity().getRecord());
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UPDATE EDGE POINTERS";
  }

  /**
   * handles vertex consistency after an UPDATE EDGE
   *
   * @param record the edge record
   */
  private void handleUpdateEdge(YTDocument record) {
    Object currentOut = record.field("out");
    Object currentIn = record.field("in");

    Object prevOut = record.getOriginalValue("out");
    Object prevIn = record.getOriginalValue("in");

    if (currentOut instanceof Collection<?> col && col.size() == 1) {
      currentOut = col.iterator().next();
      record.setPropertyInternal("out", currentOut);
    }
    if (currentIn instanceof Collection<?> col && col.size() == 1) {
      currentIn = col.iterator().next();
      record.setPropertyInternal("in", currentIn);
    }

    validateOutInForEdge(currentOut, currentIn);

    var prevInIdentifiable = (YTIdentifiable) prevIn;
    var currentInIdentifiable = (YTIdentifiable) currentIn;
    var currentOutIdentifiable = (YTIdentifiable) currentOut;
    var prevOutIdentifiable = (YTIdentifiable) prevOut;

    YTVertexInternal.changeVertexEdgePointers(
        record,
        prevInIdentifiable,
        currentInIdentifiable,
        prevOutIdentifiable,
        currentOutIdentifiable);
  }

  private static void validateOutInForEdge(Object currentOut, Object currentIn) {
    if (recordIsNotInstanceOfVertex(currentOut)) {
      throw new YTCommandExecutionException(
          "Error updating edge: 'out' is not a vertex - " + currentOut);
    }
    if (recordIsNotInstanceOfVertex(currentIn)) {
      throw new YTCommandExecutionException(
          "Error updating edge: 'in' is not a vertex - " + currentIn);
    }
  }

  /**
   * checks if an object is a vertex
   *
   * @param iRecord The record object
   */
  private static boolean recordIsNotInstanceOfVertex(Object iRecord) {
    if (iRecord == null) {
      return true;
    }
    if (!(iRecord instanceof YTIdentifiable)) {
      return true;
    }
    try {
      YTDocument record = ((YTIdentifiable) iRecord).getRecord();
      return (!ODocumentInternal.getImmutableSchemaClass(record)
          .isSubClassOf(YTClass.VERTEX_CLASS_NAME));
    } catch (YTRecordNotFoundException rnf) {
      return true;
    }
  }
}
