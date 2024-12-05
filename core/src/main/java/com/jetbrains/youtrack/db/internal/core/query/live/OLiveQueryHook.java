/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.query.live;

import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.QUERY_LIVE_SUPPORT;

import com.jetbrains.youtrack.db.internal.common.concur.resource.OCloseable;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class OLiveQueryHook {

  public static class OLiveQueryOps implements OCloseable {

    protected Map<YTDatabaseSession, List<ORecordOperation>> pendingOps = new ConcurrentHashMap<>();
    private OLiveQueryQueueThread queueThread = new OLiveQueryQueueThread();
    private final Object threadLock = new Object();

    @Override
    public void close() {
      queueThread.stopExecution();
      try {
        queueThread.join();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      pendingOps.clear();
    }

    public OLiveQueryQueueThread getQueueThread() {
      return queueThread;
    }
  }

  public static OLiveQueryOps getOpsReference(YTDatabaseSessionInternal db) {
    return db.getSharedContext().getLiveQueryOps();
  }

  public static Integer subscribe(
      Integer token, OLiveQueryListener iListener, YTDatabaseSessionInternal db) {
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      LogManager.instance()
          .warn(
              db,
              "Live query support is disabled impossible to subscribe a listener, set '%s' to true"
                  + " for enable the live query support",
              QUERY_LIVE_SUPPORT.getKey());
      return -1;
    }
    OLiveQueryOps ops = getOpsReference(db);
    synchronized (ops.threadLock) {
      if (!ops.queueThread.isAlive()) {
        ops.queueThread = ops.queueThread.clone();
        ops.queueThread.start();
      }
    }

    return ops.queueThread.subscribe(token, iListener);
  }

  public static void unsubscribe(Integer id, YTDatabaseSessionInternal db) {
    if (Boolean.FALSE.equals(db.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      LogManager.instance()
          .warn(
              db,
              "Live query support is disabled impossible to unsubscribe a listener, set '%s' to"
                  + " true for enable the live query support",
              QUERY_LIVE_SUPPORT.getKey());
      return;
    }
    try {
      OLiveQueryOps ops = getOpsReference(db);
      synchronized (ops.threadLock) {
        ops.queueThread.unsubscribe(id);
      }
    } catch (Exception e) {
      LogManager.instance().warn(OLiveQueryHook.class, "Error on unsubscribing client", e);
    }
  }

  public static void notifyForTxChanges(YTDatabaseSession iDatabase) {

    OLiveQueryOps ops = getOpsReference((YTDatabaseSessionInternal) iDatabase);
    if (ops.pendingOps.isEmpty()) {
      return;
    }
    if (Boolean.FALSE.equals(iDatabase.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      return;
    }

    List<ORecordOperation> list;
    synchronized (ops.pendingOps) {
      list = ops.pendingOps.remove(iDatabase);
    }
    // TODO sync
    if (list != null) {
      for (ORecordOperation item : list) {
        final RecordAbstract record = item.record.copy();
        item.record = record;
        ops.queueThread.enqueue(item);
      }
    }
  }

  public static void removePendingDatabaseOps(YTDatabaseSession iDatabase) {
    try {
      if (iDatabase.isClosed()
          || Boolean.FALSE.equals(iDatabase.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
        return;
      }
      OLiveQueryOps ops = getOpsReference((YTDatabaseSessionInternal) iDatabase);
      synchronized (ops.pendingOps) {
        ops.pendingOps.remove(iDatabase);
      }
    } catch (YTDatabaseException ex) {
      // This catch and log the exception because in some case is suppressing the real exception
      LogManager.instance().error(iDatabase, "Error cleaning the live query resources", ex);
    }
  }

  public static void addOp(EntityImpl iDocument, byte iType, YTDatabaseSession database) {
    var db = database;
    OLiveQueryOps ops = getOpsReference((YTDatabaseSessionInternal) db);
    if (!ops.queueThread.hasListeners()) {
      return;
    }
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      return;
    }
    ORecordOperation result = new ORecordOperation(iDocument, iType);
    synchronized (ops.pendingOps) {
      List<ORecordOperation> list = ops.pendingOps.get(db);
      if (list == null) {
        list = new ArrayList<ORecordOperation>();
        ops.pendingOps.put(db, list);
      }
      list.add(result);
    }
  }
}
