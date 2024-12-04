package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;

/**
 *
 */
public class ODatabaseDocumentTxInternal {

  private ODatabaseDocumentTxInternal() {
  }

  public static ODatabaseSessionInternal getInternal(ODatabaseSessionInternal db) {
    if (db instanceof ODatabaseDocumentTx) {
      db = ((ODatabaseDocumentTx) db).internal;
    }
    return db;
  }

  public static ODatabaseDocumentTx wrap(ODatabaseSessionInternal database) {
    return new ODatabaseDocumentTx(database, null);
  }

  public static YouTrackDBInternal getOrCreateEmbeddedFactory(
      String databaseDirectory, YouTrackDBConfig config) {
    return ODatabaseDocumentTx.getOrCreateEmbeddedFactory(databaseDirectory, config);
  }

  public static YouTrackDBInternal getOrCreateRemoteFactory(String url) {
    return ODatabaseDocumentTx.getOrCreateRemoteFactory(url);
  }

  public static void closeAllOnShutdown() {
    ODatabaseDocumentTx.embeddedLock.lock();
    try {
      for (YouTrackDBInternal factory : ODatabaseDocumentTx.embedded.values()) {
        factory.internalClose();
      }
      ODatabaseDocumentTx.embedded.clear();
    } finally {
      ODatabaseDocumentTx.embeddedLock.unlock();
    }

    ODatabaseDocumentTx.remoteLock.lock();
    try {
      ODatabaseDocumentTx.remote.clear();
    } finally {
      ODatabaseDocumentTx.remoteLock.unlock();
    }
  }
}
