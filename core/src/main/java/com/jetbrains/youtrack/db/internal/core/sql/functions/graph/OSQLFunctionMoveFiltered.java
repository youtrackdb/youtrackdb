package com.jetbrains.youtrack.db.internal.core.sql.functions.graph;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.io.OIOUtils;
import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionFiltered;

/**
 *
 */
public abstract class OSQLFunctionMoveFiltered extends OSQLFunctionMove
    implements OSQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  public OSQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  @Override
  public Object execute(
      final Object iThis,
      final YTIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final Iterable<YTIdentifiable> iPossibleResults,
      final CommandContext iContext) {
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
            return move(iContext.getDatabase(), iArgument, labels, iPossibleResults);
          }
        },
        iThis,
        iContext);
  }

  protected abstract Object move(
      YTDatabaseSession graph,
      YTIdentifiable iArgument,
      String[] labels,
      Iterable<YTIdentifiable> iPossibleResults);
}
