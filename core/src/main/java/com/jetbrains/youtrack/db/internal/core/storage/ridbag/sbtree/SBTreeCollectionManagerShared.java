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

package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.util.RawPairObjectInteger;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBShutdownListener;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBStartupListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.cache.AbstractWriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.ReadCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.local.WOWCache;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global.BTreeBonsaiGlobal;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsai;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsaiLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 *
 */
public final class SBTreeCollectionManagerShared
    implements SBTreeCollectionManager, YouTrackDBStartupListener, YouTrackDBShutdownListener {

  public static final String FILE_EXTENSION = ".grb";
  public static final String FILE_NAME_PREFIX = "global_collection_";

  private final AbstractPaginatedStorage storage;

  private final ConcurrentHashMap<Integer, BTree> fileIdBTreeMap = new ConcurrentHashMap<>();

  private final AtomicLong ridBagIdCounter = new AtomicLong();

  public SBTreeCollectionManagerShared(AbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  public void load() {
    final WriteCache writeCache = storage.getWriteCache();

    for (final Map.Entry<String, Long> entry : writeCache.files().entrySet()) {
      final String fileName = entry.getKey();
      if (fileName.endsWith(FILE_EXTENSION) && fileName.startsWith(FILE_NAME_PREFIX)) {
        final BTree bTree =
            new BTree(
                storage,
                fileName.substring(0, fileName.length() - FILE_EXTENSION.length()),
                FILE_EXTENSION);
        bTree.load();
        fileIdBTreeMap.put(AbstractWriteCache.extractFileId(entry.getValue()), bTree);
        final EdgeKey edgeKey = bTree.firstKey();

        if (edgeKey != null && edgeKey.ridBagId < 0 && ridBagIdCounter.get() < -edgeKey.ridBagId) {
          ridBagIdCounter.set(-edgeKey.ridBagId);
        }
      }
    }
  }

  public void migrate() throws IOException {
    final WriteCache writeCache = storage.getWriteCache();
    final Map<String, Long> files = writeCache.files();

    final AtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();

    final List<String> filesToMigrate = new ArrayList<>();
    for (final Map.Entry<String, Long> entry : files.entrySet()) {
      final String name = entry.getKey();
      if (name.startsWith("collections_") && name.endsWith(".sbc")) {
        filesToMigrate.add(name);
      }
    }

    if (!filesToMigrate.isEmpty()) {
      LogManager.instance()
          .info(
              this,
              "There are found %d RidBags (containers for edges which are going to be migrated)."
                  + " PLEASE DO NOT SHUTDOWN YOUR DATABASE DURING MIGRATION BECAUSE THAT RISKS TO"
                  + " DAMAGE YOUR DATA !!!",
              filesToMigrate.size());
    } else {
      return;
    }

    int migrationCounter = 0;
    for (String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      LogManager.instance()
          .info(
              this,
              "Migration of RidBag for cluster #%s is started ... "
                  + "PLEASE WAIT FOR COMPLETION !",
              clusterIdStr);
      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      atomicOperationsManager.executeInsideAtomicOperation(null, bTree::create);

      final SBTreeBonsaiLocal<Identifiable, Integer> bonsaiLocal =
          new SBTreeBonsaiLocal<>(
              fileName.substring(0, fileName.length() - ".sbc".length()), ".sbc", storage);
      bonsaiLocal.load(LinkSerializer.INSTANCE, IntegerSerializer.INSTANCE);

      final List<BonsaiBucketPointer> roots = bonsaiLocal.loadRoots();
      for (final BonsaiBucketPointer root : roots) {
        bonsaiLocal.forEachItem(
            root,
            pair -> {
              try {
                atomicOperationsManager.executeInsideAtomicOperation(
                    null,
                    atomicOperation -> {
                      final RID rid = pair.first.getIdentity();

                      bTree.put(
                          atomicOperation,
                          new EdgeKey(
                              (root.getPageIndex() << 16) + root.getPageOffset(),
                              rid.getClusterId(),
                              rid.getClusterPosition()),
                          pair.second);
                    });
              } catch (final IOException e) {
                throw BaseException.wrapException(
                    new StorageException("Error during migration of RidBag data"), e);
              }
            });
      }

      migrationCounter++;
      LogManager.instance()
          .info(
              this,
              "%d RidBags out of %d are migrated ... PLEASE WAIT FOR COMPLETION !",
              migrationCounter,
              filesToMigrate.size());
    }

    LogManager.instance()
        .info(this, "All RidBags are going to be flushed out ... PLEASE WAIT FOR COMPLETION !");
    final ReadCache readCache = storage.getReadCache();

    int flushCounter = 0;
    for (final String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      final String newFileName = generateLockName(clusterId);

      final long fileId = writeCache.fileIdByName(fileName);
      final long newFileId = writeCache.fileIdByName(newFileName);

      readCache.closeFile(fileId, false, writeCache);
      readCache.closeFile(newFileId, true, writeCache);

      // old file is removed and id of this file is used as id of the new file
      // new file keeps the same name
      writeCache.replaceFileId(fileId, newFileId);
      flushCounter++;

      LogManager.instance()
          .info(
              this,
              "%d RidBags are flushed out of %d ... PLEASE WAIT FOR COMPLETION !",
              flushCounter,
              filesToMigrate.size());
    }

    for (final String fileName : filesToMigrate) {
      final String clusterIdStr =
          fileName.substring("collections_".length(), fileName.length() - ".sbc".length());
      final int clusterId = Integer.parseInt(clusterIdStr);

      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      bTree.load();

      fileIdBTreeMap.put(WOWCache.extractFileId(bTree.getFileId()), bTree);
    }

    LogManager.instance().info(this, "All RidBags are migrated.");
  }

  @Override
  public SBTreeBonsai<Identifiable, Integer> createAndLoadTree(
      final AtomicOperation atomicOperation, final int clusterId) {
    return doCreateRidBag(atomicOperation, clusterId);
  }

  public boolean isComponentPresent(final AtomicOperation operation, final int clusterId) {
    return operation.fileIdByName(generateLockName(clusterId)) >= 0;
  }

  public void createComponent(final AtomicOperation operation, final int clusterId) {
    // lock is already acquired on storage level, during storage open

    final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
    bTree.create(operation);

    final int intFileId = WOWCache.extractFileId(bTree.getFileId());
    fileIdBTreeMap.put(intFileId, bTree);
  }

  public void deleteComponentByClusterId(
      final AtomicOperation atomicOperation, final int clusterId) {
    // lock is already acquired on storage level, during cluster drop

    final long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));
    final int intFileId = AbstractWriteCache.extractFileId(fileId);
    final BTree bTree = fileIdBTreeMap.remove(intFileId);
    if (bTree != null) {
      bTree.delete(atomicOperation);
    }
  }

  private BTreeBonsaiGlobal doCreateRidBag(AtomicOperation atomicOperation, int clusterId) {
    long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));

    // lock is already acquired on storage level, during start fo the transaction so we
    // are thread safe here.
    if (fileId < 0) {
      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      bTree.create(atomicOperation);

      fileId = bTree.getFileId();
      final long nextRidBagId = -ridBagIdCounter.incrementAndGet();

      final int intFileId = AbstractWriteCache.extractFileId(fileId);
      fileIdBTreeMap.put(intFileId, bTree);

      return new BTreeBonsaiGlobal(
          bTree, intFileId, nextRidBagId, LinkSerializer.INSTANCE, IntegerSerializer.INSTANCE);
    } else {
      final int intFileId = AbstractWriteCache.extractFileId(fileId);
      final BTree bTree = fileIdBTreeMap.get(intFileId);
      final long nextRidBagId = -ridBagIdCounter.incrementAndGet();

      return new BTreeBonsaiGlobal(
          bTree, intFileId, nextRidBagId, LinkSerializer.INSTANCE, IntegerSerializer.INSTANCE);
    }
  }

  @Override
  public SBTreeBonsai<Identifiable, Integer> loadSBTree(
      BonsaiCollectionPointer collectionPointer) {
    final int intFileId = AbstractWriteCache.extractFileId(collectionPointer.getFileId());

    final BTree bTree = fileIdBTreeMap.get(intFileId);

    final long ridBagId;
    final BonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    if (rootPointer.getPageIndex() < 0) {
      ridBagId = rootPointer.getPageIndex();
    } else {
      ridBagId = (rootPointer.getPageIndex() << 16) + rootPointer.getPageOffset();
    }

    return new BTreeBonsaiGlobal(
        bTree, intFileId, ridBagId, LinkSerializer.INSTANCE, IntegerSerializer.INSTANCE);
  }

  @Override
  public void releaseSBTree(final BonsaiCollectionPointer collectionPointer) {
  }

  @Override
  public void delete(final BonsaiCollectionPointer collectionPointer) {
  }

  @Override
  public BonsaiCollectionPointer createSBTree(
      int clusterId, AtomicOperation atomicOperation, UUID ownerUUID) {
    final BTreeBonsaiGlobal bonsaiGlobal = doCreateRidBag(atomicOperation, clusterId);
    final BonsaiCollectionPointer pointer = bonsaiGlobal.getCollectionPointer();

    if (ownerUUID != null) {
      Map<UUID, BonsaiCollectionPointer> changedPointers =
          DatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return pointer;
  }

  /**
   * Change UUID to null to prevent its serialization to disk.
   */
  @Override
  public UUID listenForChanges(RidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final BonsaiCollectionPointer pointer = collection.getPointer();
      DatabaseSessionInternal session = DatabaseRecordThreadLocal.instance().get();
      Map<UUID, BonsaiCollectionPointer> changedPointers = session.getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, BonsaiCollectionPointer pointer) {
  }

  @Override
  public void clearPendingCollections() {
  }

  @Override
  public Map<UUID, BonsaiCollectionPointer> changedIds() {
    return DatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
  }

  @Override
  public void clearChangedIds() {
    DatabaseRecordThreadLocal.instance().get().getCollectionsChanges().clear();
  }

  @Override
  public void onShutdown() {
  }

  @Override
  public void onStartup() {
  }

  public void close() {
    fileIdBTreeMap.clear();
  }

  public boolean delete(
      AtomicOperation atomicOperation, BonsaiCollectionPointer collectionPointer) {
    final int fileId = (int) collectionPointer.getFileId();
    final BTree bTree = fileIdBTreeMap.get(fileId);
    if (bTree == null) {
      throw new StorageException(
          "RidBug for with collection pointer " + collectionPointer + " does not exist");
    }

    final long ridBagId;
    final BonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    if (rootPointer.getPageIndex() < 0) {
      ridBagId = rootPointer.getPageIndex();
    } else {
      ridBagId = (rootPointer.getPageIndex() << 16) + rootPointer.getPageOffset();
    }

    try (Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }

    return true;
  }

  /**
   * Generates a lock name for the given cluster ID.
   *
   * @param clusterId the cluster ID to generate the lock name for.
   * @return the generated lock name.
   */
  public static String generateLockName(int clusterId) {
    return FILE_NAME_PREFIX + clusterId + FILE_EXTENSION;
  }
}
