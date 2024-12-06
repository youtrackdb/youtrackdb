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

package com.jetbrains.youtrack.db.internal.client.remote;

import com.jetbrains.youtrack.db.internal.common.concur.resource.CloseableInStorage;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBShutdownListener;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBStartupListener;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsai;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.SBTreeCollectionManager;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class SBTreeCollectionManagerRemote
    implements CloseableInStorage,
    SBTreeCollectionManager,
    YouTrackDBStartupListener,
    YouTrackDBShutdownListener {

  private volatile ThreadLocal<Map<UUID, WeakReference<RidBag>>> pendingCollections =
      new PendingCollectionsThreadLocal();

  public SBTreeCollectionManagerRemote() {

    YouTrackDBManager.instance().registerWeakYouTrackDBStartupListener(this);
    YouTrackDBManager.instance().registerWeakYouTrackDBShutdownListener(this);
  }

  @Override
  public void onShutdown() {
    pendingCollections = null;
  }

  @Override
  public void onStartup() {
    if (pendingCollections == null) {
      pendingCollections = new PendingCollectionsThreadLocal();
    }
  }

  protected SBTreeBonsai<Identifiable, Integer> createEdgeTree(
      AtomicOperation atomicOperation, final int clusterId) {
    throw new UnsupportedOperationException(
        "Creation of SB-Tree from remote storage is not allowed");
  }

  protected SBTreeBonsai<Identifiable, Integer> loadTree(
      BonsaiCollectionPointer collectionPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UUID listenForChanges(RidBag collection) {
    UUID id = collection.getTemporaryId();
    if (id == null) {
      id = UUID.randomUUID();
    }

    pendingCollections.get().put(id, new WeakReference<RidBag>(collection));

    return id;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, BonsaiCollectionPointer pointer) {
    final WeakReference<RidBag> reference = pendingCollections.get().get(uuid);
    if (reference == null) {
      LogManager.instance()
          .warn(this, "Update of collection pointer is received but collection is not registered");
      return;
    }

    final RidBag collection = reference.get();

    if (collection != null) {
      collection.notifySaved(pointer);
    }
  }

  @Override
  public void clearPendingCollections() {
    pendingCollections.get().clear();
  }

  @Override
  public Map<UUID, BonsaiCollectionPointer> changedIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearChangedIds() {
    throw new UnsupportedOperationException();
  }

  private static class PendingCollectionsThreadLocal
      extends ThreadLocal<Map<UUID, WeakReference<RidBag>>> {

    @Override
    protected Map<UUID, WeakReference<RidBag>> initialValue() {
      return new HashMap<UUID, WeakReference<RidBag>>();
    }
  }

  @Override
  public SBTreeBonsai<Identifiable, Integer> createAndLoadTree(
      AtomicOperation atomicOperation, int clusterId) throws IOException {
    return loadSBTree(createSBTree(clusterId, atomicOperation, null));
  }

  @Override
  public BonsaiCollectionPointer createSBTree(
      int clusterId, AtomicOperation atomicOperation, UUID ownerUUID) throws IOException {
    SBTreeBonsai<Identifiable, Integer> tree = createEdgeTree(atomicOperation, clusterId);
    return tree.getCollectionPointer();
  }

  @Override
  public SBTreeBonsai<Identifiable, Integer> loadSBTree(
      BonsaiCollectionPointer collectionPointer) {

    final SBTreeBonsai<Identifiable, Integer> tree;
    tree = loadTree(collectionPointer);

    return tree;
  }

  @Override
  public void releaseSBTree(BonsaiCollectionPointer collectionPointer) {
  }

  @Override
  public void delete(BonsaiCollectionPointer collectionPointer) {
  }

  @Override
  public void close() {
    clear();
  }

  public void clear() {
  }

  void clearClusterCache(final long fileId, String fileName) {
  }

  int size() {
    return 0;
  }
}
