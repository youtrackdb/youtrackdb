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

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsai;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public interface SBTreeCollectionManager {

  SBTreeBonsai<Identifiable, Integer> createAndLoadTree(
      AtomicOperation atomicOperation, int clusterId) throws IOException;

  BonsaiCollectionPointer createSBTree(
      int clusterId, AtomicOperation atomicOperation, UUID ownerUUID) throws IOException;

  SBTreeBonsai<Identifiable, Integer> loadSBTree(BonsaiCollectionPointer collectionPointer);

  void releaseSBTree(BonsaiCollectionPointer collectionPointer);

  void delete(BonsaiCollectionPointer collectionPointer);

  UUID listenForChanges(RidBag collection);

  void updateCollectionPointer(UUID uuid, BonsaiCollectionPointer pointer);

  void clearPendingCollections();

  Map<UUID, BonsaiCollectionPointer> changedIds();

  void clearChangedIds();
}
