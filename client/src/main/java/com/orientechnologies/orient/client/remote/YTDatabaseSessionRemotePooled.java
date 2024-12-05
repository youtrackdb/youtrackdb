package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.db.document.YTDatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.core.db.ODatabasePoolInternal;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.OSharedContext;

/**
 *
 */
public class YTDatabaseSessionRemotePooled extends YTDatabaseSessionRemote {

  private final ODatabasePoolInternal pool;

  public YTDatabaseSessionRemotePooled(
      ODatabasePoolInternal pool, StorageRemote storage, OSharedContext sharedContext) {
    super(storage, sharedContext);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed()) {
      return;
    }

    internalClose(true);
    pool.release(this);
  }

  @Override
  public YTDatabaseSessionInternal copy() {
    return (YTDatabaseSessionInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(YTDatabaseSession.STATUS.OPEN);
  }

  public void realClose() {
    YTDatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      activateOnCurrentThread();
      super.close();
    } finally {
      if (old == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(old);
      }
    }
  }
}
