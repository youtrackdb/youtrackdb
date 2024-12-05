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
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityEntry;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.LiveQueryListenerImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLProjectionItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nonnull;

public class OLiveQueryHookV2 {

  public static class OLiveQueryOp {

    public YTResult before;
    public YTResult after;
    public byte type;
    protected EntityImpl originalDoc;

    OLiveQueryOp(EntityImpl originalDoc, YTResult before, YTResult after, byte type) {
      this.originalDoc = originalDoc;
      this.type = type;
      this.before = before;
      this.after = after;
    }
  }

  public static class OLiveQueryOps implements OCloseable {

    protected Map<YTDatabaseSession, List<OLiveQueryOp>> pendingOps = new ConcurrentHashMap<>();
    private OLiveQueryQueueThreadV2 queueThread = new OLiveQueryQueueThreadV2(this);
    private final Object threadLock = new Object();

    private final BlockingQueue<OLiveQueryOp> queue = new LinkedBlockingQueue<OLiveQueryOp>();
    private final ConcurrentMap<Integer, OLiveQueryListenerV2> subscribers =
        new ConcurrentHashMap<Integer, OLiveQueryListenerV2>();

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

    public OLiveQueryQueueThreadV2 getQueueThread() {
      return queueThread;
    }

    public Map<Integer, OLiveQueryListenerV2> getSubscribers() {
      return subscribers;
    }

    public BlockingQueue<OLiveQueryOp> getQueue() {
      return queue;
    }

    public void enqueue(OLiveQueryHookV2.OLiveQueryOp item) {
      queue.offer(item);
    }

    public Integer subscribe(Integer id, OLiveQueryListenerV2 iListener) {
      subscribers.put(id, iListener);
      return id;
    }

    public void unsubscribe(Integer id) {
      OLiveQueryListenerV2 res = subscribers.remove(id);
      if (res != null) {
        res.onLiveResultEnd();
      }
    }

    public boolean hasListeners() {
      return !subscribers.isEmpty();
    }
  }

  public static OLiveQueryOps getOpsReference(YTDatabaseSessionInternal db) {
    return db.getSharedContext().getLiveQueryOpsV2();
  }

  public static Integer subscribe(
      Integer token, OLiveQueryListenerV2 iListener, YTDatabaseSessionInternal db) {
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

    return ops.subscribe(token, iListener);
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
        ops.unsubscribe(id);
      }
    } catch (Exception e) {
      LogManager.instance().warn(OLiveQueryHookV2.class, "Error on unsubscribing client", e);
    }
  }

  public static void notifyForTxChanges(YTDatabaseSession database) {
    OLiveQueryOps ops = getOpsReference((YTDatabaseSessionInternal) database);
    if (ops.pendingOps.isEmpty()) {
      return;
    }
    List<OLiveQueryOp> list;
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      return;
    }
    synchronized (ops.pendingOps) {
      list = ops.pendingOps.remove(database);
    }
    // TODO sync
    if (list != null) {
      for (OLiveQueryOp item : list) {
        ops.enqueue(item);
      }
    }
  }

  public static void removePendingDatabaseOps(YTDatabaseSession database) {
    try {
      if (database.isClosed()
          || Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
        return;
      }
      OLiveQueryOps ops = getOpsReference((YTDatabaseSessionInternal) database);
      synchronized (ops.pendingOps) {
        ops.pendingOps.remove(database);
      }
    } catch (YTDatabaseException ex) {
      // This catch and log the exception because in some case is suppressing the real exception
      LogManager.instance().error(database, "Error cleaning the live query resources", ex);
    }
  }

  public static void addOp(YTDatabaseSessionInternal database, EntityImpl iDocument, byte iType) {
    OLiveQueryOps ops = getOpsReference(database);
    if (!ops.hasListeners()) {
      return;
    }
    if (Boolean.FALSE.equals(database.getConfiguration().getValue(QUERY_LIVE_SUPPORT))) {
      return;
    }

    Set<String> projectionsToLoad = calculateProjections(ops);

    YTResult before =
        iType == ORecordOperation.CREATED ? null
            : calculateBefore(database, iDocument, projectionsToLoad);
    YTResult after =
        iType == ORecordOperation.DELETED ? null
            : calculateAfter(database, iDocument, projectionsToLoad);

    OLiveQueryOp result = new OLiveQueryOp(iDocument, before, after, iType);
    synchronized (ops.pendingOps) {
      List<OLiveQueryOp> list = ops.pendingOps.get(database);
      if (list == null) {
        list = new ArrayList<>();
        ops.pendingOps.put(database, list);
      }
      if (result.type == ORecordOperation.UPDATED) {
        OLiveQueryOp prev = prevousUpdate(list, result.originalDoc);
        if (prev == null) {
          list.add(result);
        } else {
          prev.after = result.after;
        }
      } else {
        list.add(result);
      }
    }
  }

  /**
   * get all the projections that are needed by the live queries. Null means all
   *
   * @param ops
   * @return
   */
  private static Set<String> calculateProjections(OLiveQueryOps ops) {
    Set<String> result = new HashSet<>();
    if (ops == null || ops.subscribers == null) {
      return null;
    }
    for (OLiveQueryListenerV2 listener : ops.subscribers.values()) {
      if (listener instanceof LiveQueryListenerImpl) {
        SQLSelectStatement query = ((LiveQueryListenerImpl) listener).getStatement();
        SQLProjection proj = query.getProjection();
        if (proj == null || proj.getItems() == null || proj.getItems().isEmpty()) {
          return null;
        }
        for (SQLProjectionItem item : proj.getItems()) {
          if (!item.getExpression().isBaseIdentifier()) {
            return null;
          }
          result.add(item.getExpression().getDefaultAlias().getStringValue());
        }
      }
    }
    return result;
  }

  private static OLiveQueryOp prevousUpdate(List<OLiveQueryOp> list, EntityImpl doc) {
    for (OLiveQueryOp oLiveQueryOp : list) {
      if (oLiveQueryOp.originalDoc == doc) {
        return oLiveQueryOp;
      }
    }
    return null;
  }

  public static YTResultInternal calculateBefore(
      @Nonnull YTDatabaseSessionInternal db, EntityImpl iDocument,
      Set<String> projectionsToLoad) {
    YTResultInternal result = new YTResultInternal(db);
    for (String prop : iDocument.getPropertyNamesInternal()) {
      if (projectionsToLoad == null || projectionsToLoad.contains(prop)) {
        result.setProperty(prop, unboxRidbags(iDocument.getPropertyInternal(prop)));
      }
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion());
    for (Map.Entry<String, EntityEntry> rawEntry : ODocumentInternal.rawEntries(iDocument)) {
      EntityEntry entry = rawEntry.getValue();
      if (entry.isChanged()) {
        result.setProperty(
            rawEntry.getKey(), convert(iDocument.getOriginalValue(rawEntry.getKey())));
      } else if (entry.isTrackedModified()) {
        if (entry.value instanceof EntityImpl && ((EntityImpl) entry.value).isEmbedded()) {
          result.setProperty(rawEntry.getKey(),
              calculateBefore(db, (EntityImpl) entry.value, null));
        }
      }
    }
    return result;
  }

  private static Object convert(Object originalValue) {
    if (originalValue instanceof RidBag) {
      Set result = new LinkedHashSet<>();
      ((RidBag) originalValue).forEach(result::add);
      return result;
    }
    return originalValue;
  }

  private static YTResultInternal calculateAfter(
      YTDatabaseSessionInternal db, EntityImpl iDocument, Set<String> projectionsToLoad) {
    YTResultInternal result = new YTResultInternal(db);
    for (String prop : iDocument.getPropertyNamesInternal()) {
      if (projectionsToLoad == null || projectionsToLoad.contains(prop)) {
        result.setProperty(prop, unboxRidbags(iDocument.getPropertyInternal(prop)));
      }
    }
    result.setProperty("@rid", iDocument.getIdentity());
    result.setProperty("@class", iDocument.getClassName());
    result.setProperty("@version", iDocument.getVersion() + 1);
    return result;
  }

  public static Object unboxRidbags(Object value) {
    // TODO move it to some helper class
    if (value instanceof RidBag) {
      List<YTIdentifiable> result = new ArrayList<>(((RidBag) value).size());
      for (YTIdentifiable oIdentifiable : (RidBag) value) {
        result.add(oIdentifiable);
      }
      return result;
    }
    return value;
  }
}
