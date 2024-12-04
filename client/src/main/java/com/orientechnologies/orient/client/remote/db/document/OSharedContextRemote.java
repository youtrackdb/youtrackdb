package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.orient.client.remote.YouTrackDBRemote;
import com.orientechnologies.orient.client.remote.metadata.schema.OSchemaRemote;
import com.orientechnologies.orient.client.remote.metadata.security.OSecurityRemote;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OStringCache;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.storage.OStorageInfo;

/**
 *
 */
public class OSharedContextRemote extends OSharedContext {

  public OSharedContextRemote(OStorageInfo storage, YouTrackDBRemote orientDBRemote) {
    stringCache =
        new OStringCache(
            orientDBRemote
                .getContextConfiguration()
                .getValueAsInteger(YTGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    this.youtrackDB = orientDBRemote;
    this.storage = storage;
    schema = new OSchemaRemote();
    security = new OSecurityRemote();
    indexManager = new OIndexManagerRemote(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(youtrackDB);
    sequenceLibrary = new OSequenceLibraryImpl();
  }

  public synchronized void load(YTDatabaseSessionInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load(database);
        indexManager.load(database);
        // The Immutable snapshot should be after index and schema that require and before
        // everything else that use it
        schema.forceSnapshot(database);
        security.load(database);
        sequenceLibrary.load(database);
        schema.onPostIndexManagement(database);
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
    schema.close();
    security.close();
    indexManager.close();
    sequenceLibrary.close();
    loaded = false;
  }

  public synchronized void reload(YTDatabaseSessionInternal database) {
    schema.reload(database);
    indexManager.reload(database);
    // The Immutable snapshot should be after index and schema that require and before everything
    // else that use it
    schema.forceSnapshot(database);
    security.load(database);
    scheduler.load(database);
    sequenceLibrary.load(database);
    functionLibrary.load(database);
  }
}
