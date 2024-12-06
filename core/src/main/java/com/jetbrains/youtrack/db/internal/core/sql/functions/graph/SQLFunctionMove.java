package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.Direction;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionConfigurableAbstract;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class SQLFunctionMove extends SQLFunctionConfigurableAbstract {

  public static final String NAME = "move";

  public SQLFunctionMove() {
    super(NAME, 1, 2);
  }

  public SQLFunctionMove(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  protected abstract Object move(
      final DatabaseSession db, final Identifiable iRecord, final String[] iLabels);

  public String getSyntax(DatabaseSession session) {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(
      final Object iThis,
      final Identifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final CommandContext iContext) {

    DatabaseSession db =
        iContext != null
            ? iContext.getDatabase()
            : DatabaseRecordThreadLocal.instance().getIfDefined();

    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null) {
      labels =
          MultiValue.array(
              iParameters,
              String.class,
              new CallableFunction<Object, Object>() {

                @Override
                public Object call(final Object iArgument) {
                  return IOUtils.getStringContent(iArgument);
                }
              });
    } else {
      labels = null;
    }

    return SQLEngine.foreachRecord(
        new CallableFunction<Object, Identifiable>() {
          @Override
          public Object call(final Identifiable iArgument) {
            return move(db, iArgument, labels);
          }
        },
        iThis,
        iContext);
  }

  protected Object v2v(
      final DatabaseSession graph,
      final Identifiable iRecord,
      final Direction iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      try {
        Entity rec = iRecord.getRecord();
        if (rec.isVertex()) {
          return rec.asVertex().get().getVertices(iDirection, iLabels);
        } else {
          return null;
        }
      } catch (RecordNotFoundException rnf) {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object v2e(
      final DatabaseSession graph,
      final Identifiable iRecord,
      final Direction iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      try {
        Entity rec = iRecord.getRecord();
        if (rec.isVertex()) {
          return rec.asVertex().get().getEdges(iDirection, iLabels);
        } else {
          return null;
        }
      } catch (RecordNotFoundException rnf) {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object e2v(
      final DatabaseSession graph,
      final Identifiable iRecord,
      final Direction iDirection,
      final String[] iLabels) {
    if (iRecord != null) {

      try {
        Entity rec = iRecord.getRecord();
        if (rec.isEdge()) {
          if (iDirection == Direction.BOTH) {
            List results = new ArrayList();
            results.add(rec.asEdge().get().getVertex(Direction.OUT));
            results.add(rec.asEdge().get().getVertex(Direction.IN));
            return results;
          }
          return rec.asEdge().get().getVertex(iDirection);
        } else {
          return null;
        }
      } catch (RecordNotFoundException e) {
        return null;
      }
    } else {
      return null;
    }
  }
}
