package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.io.OIOUtils;
import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionConfigurableAbstract;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class OSQLFunctionMove extends OSQLFunctionConfigurableAbstract {

  public static final String NAME = "move";

  public OSQLFunctionMove() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMove(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  protected abstract Object move(
      final YTDatabaseSession db, final YTIdentifiable iRecord, final String[] iLabels);

  public String getSyntax(YTDatabaseSession session) {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(
      final Object iThis,
      final YTIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final CommandContext iContext) {

    YTDatabaseSession db =
        iContext != null
            ? iContext.getDatabase()
            : ODatabaseRecordThreadLocal.instance().getIfDefined();

    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null) {
      labels =
          OMultiValue.array(
              iParameters,
              String.class,
              new OCallable<Object, Object>() {

                @Override
                public Object call(final Object iArgument) {
                  return OIOUtils.getStringContent(iArgument);
                }
              });
    } else {
      labels = null;
    }

    return OSQLEngine.foreachRecord(
        new OCallable<Object, YTIdentifiable>() {
          @Override
          public Object call(final YTIdentifiable iArgument) {
            return move(db, iArgument, labels);
          }
        },
        iThis,
        iContext);
  }

  protected Object v2v(
      final YTDatabaseSession graph,
      final YTIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      try {
        Entity rec = iRecord.getRecord();
        if (rec.isVertex()) {
          return rec.asVertex().get().getVertices(iDirection, iLabels);
        } else {
          return null;
        }
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object v2e(
      final YTDatabaseSession graph,
      final YTIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      try {
        Entity rec = iRecord.getRecord();
        if (rec.isVertex()) {
          return rec.asVertex().get().getEdges(iDirection, iLabels);
        } else {
          return null;
        }
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object e2v(
      final YTDatabaseSession graph,
      final YTIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {

      try {
        Entity rec = iRecord.getRecord();
        if (rec.isEdge()) {
          if (iDirection == ODirection.BOTH) {
            List results = new ArrayList();
            results.add(rec.asEdge().get().getVertex(ODirection.OUT));
            results.add(rec.asEdge().get().getVertex(ODirection.IN));
            return results;
          }
          return rec.asEdge().get().getVertex(iDirection);
        } else {
          return null;
        }
      } catch (YTRecordNotFoundException e) {
        return null;
      }
    } else {
      return null;
    }
  }
}
