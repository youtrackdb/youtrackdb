package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OVertexInternal;
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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    var prev = this.prev;
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OResultInternal) {
      handleUpdateEdge(result.toElement().getRecord());
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
  private void handleUpdateEdge(ODocument record) {
    Object currentOut = record.field("out");
    Object currentIn = record.field("in");

    Object prevOut = record.getOriginalValue("out");
    Object prevIn = record.getOriginalValue("in");

    if (currentOut instanceof Collection<?> col && col.size() == 1) {
      currentOut = col.iterator().next();
      record.setPropertyWithoutValidation("out", currentOut);
    }
    if (currentIn instanceof Collection<?> col && col.size() == 1) {
      currentIn = col.iterator().next();
      record.setPropertyWithoutValidation("in", currentIn);
    }

    validateOutInForEdge(currentOut, currentIn);

    var prevInIdentifiable = (OIdentifiable) prevIn;
    var currentInIdentifiable = (OIdentifiable) currentIn;
    var currentOutIdentifiable = (OIdentifiable) currentOut;
    var prevOutIdentifiable = (OIdentifiable) prevOut;

    OVertexInternal.changeVertexEdgePointers(
        record,
        prevInIdentifiable,
        currentInIdentifiable,
        prevOutIdentifiable,
        currentOutIdentifiable);
  }

  private void validateOutInForEdge(Object currentOut, Object currentIn) {
    if (recordIsNotInstanceOfVertex(currentOut)) {
      throw new OCommandExecutionException(
          "Error updating edge: 'out' is not a vertex - " + currentOut);
    }
    if (recordIsNotInstanceOfVertex(currentIn)) {
      throw new OCommandExecutionException(
          "Error updating edge: 'in' is not a vertex - " + currentIn);
    }
  }

  /**
   * checks if an object is a vertex
   *
   * @param iRecord The record object
   */
  private boolean recordIsNotInstanceOfVertex(Object iRecord) {
    if (iRecord == null) {
      return true;
    }
    if (!(iRecord instanceof OIdentifiable)) {
      return true;
    }
    ODocument record = ((OIdentifiable) iRecord).getRecord();
    if (record == null) {
      return true;
    }
    return (!ODocumentInternal.getImmutableSchemaClass(record)
        .isSubClassOf(OClass.VERTEX_CLASS_NAME));
  }
}
