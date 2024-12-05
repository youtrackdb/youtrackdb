package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.index.OIndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerShared;
import com.jetbrains.youtrack.db.internal.core.index.OIndexes;
import com.jetbrains.youtrack.db.internal.core.index.YTIndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunctionLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.OSchemaEmbedded;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.OSequenceLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHook;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHookV2;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.schedule.OSchedulerImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OQueryStats;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExecutionPlanCache;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatementCache;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class OSharedContextEmbedded extends OSharedContext {

  protected Map<String, DistributedQueryContext> activeDistributedQueries;
  protected ViewManager viewManager;

  public OSharedContextEmbedded(OStorage storage, YouTrackDBEmbedded youtrackDB) {
    this.youtrackDB = youtrackDB;
    this.storage = storage;
    init(storage);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new OSchemaEmbedded();
    security = youtrackDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new OIndexManagerShared(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(youtrackDB);
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    statementCache =
        new OStatementCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new OExecutionPlanCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.STATEMENT_CACHE_SIZE));
    this.registerListener(executionPlanCache);

    queryStats = new OQueryStats();
    activeDistributedQueries = new HashMap<>();
    ((OAbstractPaginatedStorage) storage)
        .setStorageConfigurationUpdateListener(
            update -> {
              for (OMetadataUpdateListener listener : browseListeners()) {
                listener.onStorageConfigurationUpdate(storage.getName(), update);
              }
            });

    this.viewManager = new ViewManager(youtrackDB, storage.getName());
  }

  public synchronized void load(YTDatabaseSessionInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load(database);
        schema.forceSnapshot(database);
        indexManager.load(database);
        // The Immutable snapshot should be after index and schema that require and before
        // everything else that use it
        schema.forceSnapshot(database);
        security.load(database);
        functionLibrary.load(database);
        scheduler.load(database);
        sequenceLibrary.load(database);
        schema.onPostIndexManagement(database);
        viewManager.load();
        loaded = true;
      }
    } finally {
      PROFILER.stopChrono(
          PROFILER.getDatabaseMetric(database.getName(), "metadata.load"),
          "Loading of database metadata",
          timer,
          "db.*.metadata.load");
    }
  }

  @Override
  public synchronized void close() {
    stringCache.close();
    viewManager.close();
    schema.close();
    security.close();
    indexManager.close();
    functionLibrary.close();
    scheduler.close();
    sequenceLibrary.close();
    statementCache.clear();
    executionPlanCache.invalidate();
    liveQueryOps.close();
    liveQueryOpsV2.close();
    activeDistributedQueries.values().forEach(DistributedQueryContext::close);
    loaded = false;
  }

  public synchronized void reload(YTDatabaseSessionInternal database) {
    schema.reload(database);
    indexManager.reload(database);
    // The Immutable snapshot should be after index and schema that require and before everything
    // else that use it
    schema.forceSnapshot(database);
    security.load(database);
    functionLibrary.load(database);
    sequenceLibrary.load(database);
    scheduler.load(database);
  }

  public synchronized void create(YTDatabaseSessionInternal database) {
    schema.create(database);
    indexManager.create(database);
    security.create(database);
    functionLibrary.create(database);
    OSequenceLibraryImpl.create(database);
    security.createClassTrigger(database);
    OSchedulerImpl.create(database);
    schema.forceSnapshot(database);

    // CREATE BASE VERTEX AND EDGE CLASSES
    schema.createClass(database, Entity.DEFAULT_CLASS_NAME);
    schema.createClass(database, "V");
    schema.createClass(database, "E");

    // create geospatial classes
    try {
      OIndexFactory factory = OIndexes.getFactory(YTClass.INDEX_TYPE.SPATIAL.toString(), "LUCENE");
      if (factory instanceof ODatabaseLifecycleListener) {
        ((ODatabaseLifecycleListener) factory).onCreate(database);
      }
    } catch (YTIndexException x) {
      // the index does not exist
    }

    viewManager.create();
    loaded = true;
  }

  public Map<String, DistributedQueryContext> getActiveDistributedQueries() {
    return activeDistributedQueries;
  }

  public ViewManager getViewManager() {
    return viewManager;
  }

  public synchronized void reInit(
      OAbstractPaginatedStorage storage2, YTDatabaseSessionInternal database) {
    this.close();
    this.storage = storage2;
    this.init(storage2);
    ((OMetadataDefault) database.getMetadata()).init(this);
    this.load(database);
  }

  public synchronized Map<String, Object> loadConfig(
      YTDatabaseSessionInternal session, String name) {
    //noinspection unchecked
    return (Map<String, Object>)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              assert !session.getTransaction().isActive();
              String propertyName = "__config__" + name;
              String id = storage.getConfiguration().getProperty(propertyName);
              if (id != null) {
                YTRecordId recordId = new YTRecordId(id);
                EntityImpl config = session.load(recordId);
                ORecordInternal.setIdentity(config, new YTRecordId(-1, -1));
                return config.toMap();
              } else {
                return null;
              }
            });
  }

  public Map<String, Object> loadDistributedConfig(YTDatabaseSessionInternal session) {
    return loadConfig(session, "ditributedConfig");
  }
}
