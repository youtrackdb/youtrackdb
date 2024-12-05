package com.orientechnologies.core.db.document;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.db.YouTrackDBInternal;

/**
 *
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static YTDatabaseSessionInternal getInternal(YTDatabaseSessionInternal db) {
    if (db instanceof YTDatabaseDocumentTx) {
      db = ((YTDatabaseDocumentTx) db).internal;
    }
    return db;
  }

  public static YTDatabaseDocumentTx wrap(YTDatabaseSessionInternal database) {
    return new YTDatabaseDocumentTx(database, null);
  }

  public static YouTrackDBInternal getOrCreateEmbeddedFactory(
      String databaseDirectory, YouTrackDBConfig config) {
    return YTDatabaseDocumentTx.getOrCreateEmbeddedFactory(databaseDirectory, config);
  }

  public static YouTrackDBInternal getOrCreateRemoteFactory(String url) {
    return YTDatabaseDocumentTx.getOrCreateRemoteFactory(url);
  }

  public static void closeAllOnShutdown() {
    YTDatabaseDocumentTx.embeddedLock.lock();
    try {
      for (YouTrackDBInternal factory : YTDatabaseDocumentTx.embedded.values()) {
        factory.internalClose();
      }
      YTDatabaseDocumentTx.embedded.clear();
    } finally {
      YTDatabaseDocumentTx.embeddedLock.unlock();
    }

    YTDatabaseDocumentTx.remoteLock.lock();
    try {
      YTDatabaseDocumentTx.remote.clear();
    } finally {
      YTDatabaseDocumentTx.remoteLock.unlock();
    }
  }
}
