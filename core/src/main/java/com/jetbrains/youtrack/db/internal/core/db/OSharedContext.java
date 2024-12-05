package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.listener.OListenerManger;
import com.jetbrains.youtrack.db.internal.common.profiler.OProfiler;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunctionLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.OSchemaShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.OSequenceLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHook;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHookV2;
import com.jetbrains.youtrack.db.internal.core.schedule.OSchedulerImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OQueryStats;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExecutionPlanCache;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatementCache;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.OStorageInfo;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public abstract class OSharedContext extends OListenerManger<OMetadataUpdateListener> {

  protected static final OProfiler PROFILER = YouTrackDBManager.instance().getProfiler();

  protected YouTrackDBInternal youtrackDB;
  protected OStorageInfo storage;
  protected OSchemaShared schema;
  protected OSecurityInternal security;
  protected OIndexManagerAbstract indexManager;
  protected OFunctionLibraryImpl functionLibrary;
  protected OSchedulerImpl scheduler;
  protected OSequenceLibraryImpl sequenceLibrary;
  protected OLiveQueryHook.OLiveQueryOps liveQueryOps;
  protected OLiveQueryHookV2.OLiveQueryOps liveQueryOpsV2;
  protected OStatementCache statementCache;
  protected OExecutionPlanCache executionPlanCache;
  protected OQueryStats queryStats;
  protected volatile boolean loaded = false;
  protected Map<String, Object> resources;
  protected OStringCache stringCache;

  public OSharedContext() {
    super(true);
  }

  public OSchemaShared getSchema() {
    return schema;
  }

  public OSecurityInternal getSecurity() {
    return security;
  }

  public OIndexManagerAbstract getIndexManager() {
    return indexManager;
  }

  public OFunctionLibraryImpl getFunctionLibrary() {
    return functionLibrary;
  }

  public OSchedulerImpl getScheduler() {
    return scheduler;
  }

  public OSequenceLibraryImpl getSequenceLibrary() {
    return sequenceLibrary;
  }

  public OLiveQueryHook.OLiveQueryOps getLiveQueryOps() {
    return liveQueryOps;
  }

  public OLiveQueryHookV2.OLiveQueryOps getLiveQueryOpsV2() {
    return liveQueryOpsV2;
  }

  public OStatementCache getStatementCache() {
    return statementCache;
  }

  public OExecutionPlanCache getExecutionPlanCache() {
    return executionPlanCache;
  }

  public OQueryStats getQueryStats() {
    return queryStats;
  }

  public abstract void load(YTDatabaseSessionInternal oDatabaseDocumentInternal);

  public abstract void reload(YTDatabaseSessionInternal database);

  public abstract void close();

  public OStorageInfo getStorage() {
    return storage;
  }

  public YouTrackDBInternal getYouTrackDB() {
    return youtrackDB;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  public ViewManager getViewManager() {
    throw new UnsupportedOperationException();
  }

  public synchronized <T> T getResource(final String name, final Callable<T> factory) {
    if (resources == null) {
      resources = new HashMap<String, Object>();
    }
    @SuppressWarnings("unchecked")
    T resource = (T) resources.get(name);
    if (resource == null) {
      try {
        resource = factory.call();
      } catch (Exception e) {
        YTException.wrapException(
            new YTDatabaseException(String.format("instance creation for '%s' failed", name)), e);
      }
      resources.put(name, resource);
    }
    return resource;
  }

  public synchronized void reInit(
      AbstractPaginatedStorage storage2, YTDatabaseSessionInternal database) {
    throw new UnsupportedOperationException();
  }

  public OStringCache getStringCache() {
    return this.stringCache;
  }
}
