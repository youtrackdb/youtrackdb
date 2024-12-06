package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewManager;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.IndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerShared;
import com.jetbrains.youtrack.db.internal.core.index.Indexes;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.function.FunctionLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaEmbedded;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.query.live.LiveQueryHook;
import com.jetbrains.youtrack.db.internal.core.query.live.LiveQueryHookV2.LiveQueryOps;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.schedule.SchedulerImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.QueryStats;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ExecutionPlanCache;
import com.jetbrains.youtrack.db.internal.core.sql.parser.StatementCache;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SharedContextEmbedded extends SharedContext {

  protected Map<String, DistributedQueryContext> activeDistributedQueries;
  protected ViewManager viewManager;

  public SharedContextEmbedded(Storage storage, YouTrackDBEmbedded youtrackDB) {
    this.youtrackDB = youtrackDB;
    this.storage = storage;
    init(storage);
  }

  protected void init(Storage storage) {
    stringCache =
        new StringCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new SchemaEmbedded();
    security = youtrackDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new IndexManagerShared(storage);
    functionLibrary = new FunctionLibraryImpl();
    scheduler = new SchedulerImpl(youtrackDB);
    sequenceLibrary = new SequenceLibraryImpl();
    liveQueryOps = new LiveQueryHook.LiveQueryOps();
    liveQueryOpsV2 = new LiveQueryOps();
    statementCache =
        new StatementCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new ExecutionPlanCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.STATEMENT_CACHE_SIZE));
    this.registerListener(executionPlanCache);

    queryStats = new QueryStats();
    activeDistributedQueries = new HashMap<>();
    ((AbstractPaginatedStorage) storage)
        .setStorageConfigurationUpdateListener(
            update -> {
              for (MetadataUpdateListener listener : browseListeners()) {
                listener.onStorageConfigurationUpdate(storage.getName(), update);
              }
            });

    this.viewManager = new ViewManager(youtrackDB, storage.getName());
  }

  public synchronized void load(DatabaseSessionInternal database) {
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

  public synchronized void reload(DatabaseSessionInternal database) {
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

  public synchronized void create(DatabaseSessionInternal database) {
    schema.create(database);
    indexManager.create(database);
    security.create(database);
    functionLibrary.create(database);
    SequenceLibraryImpl.create(database);
    security.createClassTrigger(database);
    SchedulerImpl.create(database);
    schema.forceSnapshot(database);

    // CREATE BASE VERTEX AND EDGE CLASSES
    schema.createClass(database, Entity.DEFAULT_CLASS_NAME);
    schema.createClass(database, "V");
    schema.createClass(database, "E");

    // create geospatial classes
    try {
      IndexFactory factory = Indexes.getFactory(SchemaClass.INDEX_TYPE.SPATIAL.toString(),
          "LUCENE");
      if (factory instanceof DatabaseLifecycleListener) {
        ((DatabaseLifecycleListener) factory).onCreate(database);
      }
    } catch (IndexException x) {
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
      AbstractPaginatedStorage storage2, DatabaseSessionInternal database) {
    this.close();
    this.storage = storage2;
    this.init(storage2);
    ((MetadataDefault) database.getMetadata()).init(this);
    this.load(database);
  }

  public synchronized Map<String, Object> loadConfig(
      DatabaseSessionInternal session, String name) {
    //noinspection unchecked
    return (Map<String, Object>)
        ScenarioThreadLocal.executeAsDistributed(
            () -> {
              assert !session.getTransaction().isActive();
              String propertyName = "__config__" + name;
              String id = storage.getConfiguration().getProperty(propertyName);
              if (id != null) {
                RecordId recordId = new RecordId(id);
                EntityImpl config = session.load(recordId);
                RecordInternal.setIdentity(config, new RecordId(-1, -1));
                return config.toMap();
              } else {
                return null;
              }
            });
  }

  public Map<String, Object> loadDistributedConfig(DatabaseSessionInternal session) {
    return loadConfig(session, "ditributedConfig");
  }
}
