package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.remote.db.document.DatabaseSessionRemote;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePoolInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;

/**
 *
 */
public class DatabaseSessionRemotePooled extends DatabaseSessionRemote {

  private final DatabasePoolInternal pool;

  public DatabaseSessionRemotePooled(
      DatabasePoolInternal pool, StorageRemote storage, SharedContext sharedContext) {
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
  public DatabaseSessionInternal copy() {
    return (DatabaseSessionInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(DatabaseSession.STATUS.OPEN);
  }

  public void realClose() {
    DatabaseSessionInternal old = DatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      activateOnCurrentThread();
      super.close();
    } finally {
      if (old == null) {
        DatabaseRecordThreadLocal.instance().remove();
      } else {
        DatabaseRecordThreadLocal.instance().set(old);
      }
    }
  }
}
