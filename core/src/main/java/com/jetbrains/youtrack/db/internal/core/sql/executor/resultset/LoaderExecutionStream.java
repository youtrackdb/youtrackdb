package com.jetbrains.youtrack.db.internal.core.sql.executor.resultset;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTContextualRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import java.util.Iterator;

public final class LoaderExecutionStream implements ExecutionStream {

  private YTResult nextResult = null;
  private final Iterator<? extends YTIdentifiable> iterator;

  public LoaderExecutionStream(Iterator<? extends YTIdentifiable> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(CommandContext ctx) {
    if (nextResult == null) {
      fetchNext(ctx);
    }
    return nextResult != null;
  }

  @Override
  public YTResult next(CommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }

    YTResult result = nextResult;
    nextResult = null;
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public void close(CommandContext ctx) {
  }

  private void fetchNext(CommandContext ctx) {
    if (nextResult != null) {
      return;
    }
    var db = ctx.getDatabase();
    while (iterator.hasNext()) {
      YTIdentifiable nextRid = iterator.next();

      if (nextRid != null) {
        if (nextRid instanceof Record record) {
          nextResult = new YTResultInternal(db, record);
          return;
        } else {
          try {
            Record nextDoc = db.load(nextRid.getIdentity());
            YTResultInternal res = new YTResultInternal(db, nextDoc);
            if (nextRid instanceof YTContextualRecordId) {
              res.addMetadata(((YTContextualRecordId) nextRid).getContext());
            }
            nextResult = res;
            return;
          } catch (YTRecordNotFoundException e) {
            return;
          }
        }
      }
    }
  }
}
