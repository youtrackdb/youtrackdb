package com.orientechnologies.core.index;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.OSharedContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.document.YTDatabaseSessionEmbedded;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.storage.OStorage;
import com.orientechnologies.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class ORecreateIndexesTask implements Runnable {

  /**
   *
   */
  private final OIndexManagerShared indexManager;

  private final OSharedContext ctx;
  private int ok;
  private int errors;

  public ORecreateIndexesTask(OIndexManagerShared indexManager, OSharedContext ctx) {
    this.indexManager = indexManager;
    this.ctx = ctx;
  }

  @Override
  public void run() {
    try {
      final YTDatabaseSessionEmbedded newDb =
          new YTDatabaseSessionEmbedded((OStorage) ctx.getStorage());
      newDb.activateOnCurrentThread();
      newDb.init(null, ctx);
      newDb.internalOpen("admin", "nopass", false);

      final Collection<YTEntityImpl> indexesToRebuild;
      indexManager.acquireExclusiveLock();
      try {
        final Collection<YTEntityImpl> knownIndexes =
            indexManager.getDocument(newDb).field(OIndexManagerShared.CONFIG_INDEXES);
        if (knownIndexes == null) {
          OLogManager.instance().warn(this, "List of indexes is empty");
          indexesToRebuild = Collections.emptyList();
        } else {
          indexesToRebuild = new ArrayList<>();
          for (YTEntityImpl index : knownIndexes) {
            indexesToRebuild.add(index.copy()); // make copies to safely iterate them later
          }
        }
      } finally {
        indexManager.releaseExclusiveLock(newDb);
      }

      try {
        recreateIndexes(indexesToRebuild, newDb);
      } finally {
        if (indexManager.storage instanceof OAbstractPaginatedStorage abstractPaginatedStorage) {
          abstractPaginatedStorage.synch();
        }
        newDb.close();
      }

    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Error when attempt to restore indexes after crash was performed", e);
    }
  }

  private void recreateIndexes(
      Collection<YTEntityImpl> indexesToRebuild, YTDatabaseSessionEmbedded db) {
    ok = 0;
    errors = 0;
    for (YTEntityImpl index : indexesToRebuild) {
      try {
        recreateIndex(index, db);
      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "Error during addition of index '%s'", e, index);
        errors++;
      }
    }

    db.getMetadata().getIndexManagerInternal().save(db);

    indexManager.rebuildCompleted = true;

    OLogManager.instance()
        .info(this, "%d indexes were restored successfully, %d errors", ok, errors);
  }

  private void recreateIndex(YTEntityImpl indexDocument, YTDatabaseSessionEmbedded db) {
    final OIndexInternal index = createIndex(indexDocument);
    final OIndexMetadata indexMetadata = index.loadMetadata(indexDocument);
    final OIndexDefinition indexDefinition = indexMetadata.getIndexDefinition();

    final boolean automatic = indexDefinition != null && indexDefinition.isAutomatic();
    // XXX: At this moment Lucene-based indexes are not durable, so we still need to rebuild them.
    final boolean durable = !"LUCENE".equalsIgnoreCase(indexMetadata.getAlgorithm());

    // The database and its index manager are in a special half-open state now, the index manager
    // is created, but not populated
    // with the index metadata, we have to rebuild the whole index list manually and insert it
    // into the index manager.

    if (automatic) {
      if (durable) {
        OLogManager.instance()
            .info(
                this,
                "Index '%s' is a durable automatic index and will be added as is without"
                    + " rebuilding",
                indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      } else {
        OLogManager.instance()
            .info(
                this,
                "Index '%s' is a non-durable automatic index and must be rebuilt",
                indexMetadata.getName());
        rebuildNonDurableAutomaticIndex(db, indexDocument, index, indexMetadata, indexDefinition);
      }
    } else {
      if (durable) {
        OLogManager.instance()
            .info(
                this,
                "Index '%s' is a durable non-automatic index and will be added as is without"
                    + " rebuilding",
                indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      } else {
        OLogManager.instance()
            .info(
                this,
                "Index '%s' is a non-durable non-automatic index and will be added as is without"
                    + " rebuilding",
                indexMetadata.getName());
        addIndexAsIs(indexDocument, index, db);
      }
    }
  }

  private void rebuildNonDurableAutomaticIndex(
      YTDatabaseSessionInternal session, YTEntityImpl indexDocument,
      OIndexInternal index,
      OIndexMetadata indexMetadata,
      OIndexDefinition indexDefinition) {
    index.loadFromConfiguration(session, indexDocument);
    index.delete(session);

    final String indexName = indexMetadata.getName();
    final Set<String> clusters = indexMetadata.getClustersToIndex();
    final String type = indexMetadata.getType();

    if (clusters != null && !clusters.isEmpty() && type != null) {
      OLogManager.instance().info(this, "Start creation of index '%s'", indexName);
      index.create(session, indexMetadata, false, new OIndexRebuildOutputListener(index));

      indexManager.addIndexInternal(session, index);

      OLogManager.instance()
          .info(
              this,
              "Index '%s' was successfully created and rebuild is going to be started",
              indexName);

      index.rebuild(session, new OIndexRebuildOutputListener(index));

      ok++;

      OLogManager.instance()
          .info(this, "Rebuild of '%s index was successfully finished", indexName);
    } else {
      errors++;
      OLogManager.instance()
          .error(
              this,
              "Information about index was restored incorrectly, following data were loaded : "
                  + "index name '%s', index definition '%s', clusters %s, type %s",
              null,
              indexName,
              indexDefinition,
              clusters,
              type);
    }
  }

  private void addIndexAsIs(
      YTEntityImpl indexDocument, OIndexInternal index, YTDatabaseSessionEmbedded database) {
    if (index.loadFromConfiguration(database, indexDocument)) {
      indexManager.addIndexInternal(database, index);

      ok++;
      OLogManager.instance().info(this, "Index '%s' was added in DB index list", index.getName());
    } else {
      try {
        OLogManager.instance()
            .error(this, "Index '%s' can't be restored and will be deleted", null, index.getName());
        index.delete(database);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error while deleting index '%s'", e, index.getName());
      }
      errors++;
    }
  }

  private OIndexInternal createIndex(YTEntityImpl idx) {
    final String indexType = idx.field(OIndexInternal.CONFIG_TYPE);

    if (indexType == null) {
      OLogManager.instance().error(this, "Index type is null, will process other record", null);
      throw new YTIndexException(
          "Index type is null, will process other record. Index configuration: " + idx);
    }
    OIndexMetadata m = OIndexAbstract.loadMetadataFromDoc(idx);
    return OIndexes.createIndex(indexManager.storage, m);
  }
}
