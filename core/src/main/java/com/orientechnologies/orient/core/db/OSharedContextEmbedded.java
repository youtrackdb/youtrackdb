package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class OSharedContextEmbedded extends OSharedContext {

  protected Map<String, DistributedQueryContext> activeDistributedQueries;
  protected ViewManager viewManager;

  public OSharedContextEmbedded(OStorage storage, OxygenDBEmbedded oxygenDB) {
    this.oxygenDB = oxygenDB;
    this.storage = storage;
    init(storage);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new OSchemaEmbedded();
    security = oxygenDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new OIndexManagerShared(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(oxygenDB);
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    statementCache =
        new OStatementCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new OExecutionPlanCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
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

    this.viewManager = new ViewManager(oxygenDB, storage.getName());
  }

  public synchronized void load(ODatabaseSessionInternal database) {
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

  public synchronized void reload(ODatabaseSessionInternal database) {
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

  public synchronized void create(ODatabaseSessionInternal database) {
    schema.create(database);
    indexManager.create(database);
    security.create(database);
    functionLibrary.create(database);
    OSequenceLibraryImpl.create(database);
    security.createClassTrigger(database);
    OSchedulerImpl.create(database);
    schema.forceSnapshot(database);

    // CREATE BASE VERTEX AND EDGE CLASSES
    schema.createClass(database, OElement.DEFAULT_CLASS_NAME);
    schema.createClass(database, "V");
    schema.createClass(database, "E");

    // create geospatial classes
    try {
      OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.SPATIAL.toString(), "LUCENE");
      if (factory instanceof ODatabaseLifecycleListener) {
        ((ODatabaseLifecycleListener) factory).onCreate(database);
      }
    } catch (OIndexException x) {
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
      OAbstractPaginatedStorage storage2, ODatabaseSessionInternal database) {
    this.close();
    this.storage = storage2;
    this.init(storage2);
    ((OMetadataDefault) database.getMetadata()).init(this);
    this.load(database);
  }

  public synchronized Map<String, Object> loadConfig(
      ODatabaseSessionInternal session, String name) {
    //noinspection unchecked
    return (Map<String, Object>)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              assert !session.getTransaction().isActive();
              String propertyName = "__config__" + name;
              String id = storage.getConfiguration().getProperty(propertyName);
              if (id != null) {
                ORecordId recordId = new ORecordId(id);
                ODocument config = session.load(recordId);
                ORecordInternal.setIdentity(config, new ORecordId(-1, -1));
                return config.toMap();
              } else {
                return null;
              }
            });
  }

  /**
   * Store a configuration with a key, without checking eventual update version.
   */
  public synchronized void saveConfig(
      ODatabaseSessionInternal session, String name, Map<String, Object> value) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          assert !session.getTransaction().isActive();
          String propertyName = "__config__" + name;
          String id = storage.getConfiguration().getProperty(propertyName);
          if (id != null) {
            ORecordId recordId = new ORecordId(id);
            ODocument record;
            try {
              record = session.load(recordId);
            } catch (ORecordNotFoundException rnfe) {
              record = new ODocument();
              ORecordInternal.unsetDirty(record);
            }

            var recordVersion = record.getVersion();
            record.fromMap(value);

            ORecordInternal.setIdentity(record, recordId);
            ORecordInternal.setVersion(record, recordVersion);
            record.setDirty();

            var recordToSave = record;
            session.executeInTx(() -> session.save(recordToSave, "internal"));
          } else {
            var record = new ODocument();
            ORecordInternal.unsetDirty(record);
            record.fromMap(value);
            record.setDirty();

            ORID recordId =
                session.computeInTx(() -> session.save(record, "internal").getIdentity());
            ((OStorage) storage).setProperty(propertyName, recordId.toString());
          }
          return null;
        });
  }

  public Map<String, Object> loadDistributedConfig(ODatabaseSessionInternal session) {
    return loadConfig(session, "ditributedConfig");
  }

  public void saveDistributedConfig(
      ODatabaseSessionInternal session, String name, Map<String, Object> value) {
    this.saveConfig(session, "ditributedConfig", value);
  }
}
