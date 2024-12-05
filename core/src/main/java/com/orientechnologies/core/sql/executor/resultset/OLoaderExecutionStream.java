package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.id.YTContextualRecordId;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import java.util.Iterator;

public final class OLoaderExecutionStream implements OExecutionStream {

  private YTResult nextResult = null;
  private final Iterator<? extends YTIdentifiable> iterator;

  public OLoaderExecutionStream(Iterator<? extends YTIdentifiable> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (nextResult == null) {
      fetchNext(ctx);
    }
    return nextResult != null;
  }

  @Override
  public YTResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }

    YTResult result = nextResult;
    nextResult = null;
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
  }

  private void fetchNext(OCommandContext ctx) {
    if (nextResult != null) {
      return;
    }
    var db = ctx.getDatabase();
    while (iterator.hasNext()) {
      YTIdentifiable nextRid = iterator.next();

      if (nextRid != null) {
        if (nextRid instanceof YTRecord record) {
          nextResult = new YTResultInternal(db, record);
          return;
        } else {
          try {
            YTRecord nextDoc = db.load(nextRid.getIdentity());
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
