package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.db.document.ODatabaseSessionRemote;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OSharedContext;

/**
 *
 */
public class ODatabaseSessionRemotePooled extends ODatabaseSessionRemote {

  private final ODatabasePoolInternal pool;

  public ODatabaseSessionRemotePooled(
      ODatabasePoolInternal pool, OStorageRemote storage, OSharedContext sharedContext) {
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
  public ODatabaseSessionInternal copy() {
    return (ODatabaseSessionInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(ODatabaseSession.STATUS.OPEN);
  }

  public void realClose() {
    ODatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
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
