package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.YTContextualRecordId;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Iterator;

public final class OLoaderExecutionStream implements OExecutionStream {

  private OResult nextResult = null;
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
  public OResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }

    OResult result = nextResult;
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
          nextResult = new OResultInternal(db, record);
          return;
        } else {
          try {
            YTRecord nextDoc = db.load(nextRid.getIdentity());
            OResultInternal res = new OResultInternal(db, nextDoc);
            if (nextRid instanceof YTContextualRecordId) {
              res.addMetadata(((YTContextualRecordId) nextRid).getContext());
            }
            nextResult = res;
            return;
          } catch (ORecordNotFoundException e) {
            return;
          }
        }
      }
    }
  }
}
