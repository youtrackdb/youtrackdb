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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persistent Set<Identifiable> implementation that uses the SBTree to handle entries in persistent
 * way.
 */
public class IndexRIDContainer implements Set<Identifiable> {

  public static final String INDEX_FILE_EXTENSION = ".irs";

  private final long fileId;
  private Set<Identifiable> underlying;
  private boolean isEmbedded;
  private int topThreshold =
      GlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
  private final int bottomThreshold =
      GlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();
  private final boolean durableNonTxMode;

  /**
   * Should be called inside of lock to ensure uniqueness of entity on disk !!!
   */
  public IndexRIDContainer(String name, boolean durableNonTxMode, AtomicLong bonsayFileId) {
    long gotFileId = bonsayFileId.get();
    if (gotFileId == 0) {
      gotFileId = resolveFileIdByName(name + INDEX_FILE_EXTENSION);
      bonsayFileId.set(gotFileId);
    }
    this.fileId = gotFileId;

    underlying = new HashSet<>();
    isEmbedded = true;
    this.durableNonTxMode = durableNonTxMode;
  }

  public IndexRIDContainer(long fileId, Set<Identifiable> underlying, boolean durableNonTxMode) {
    this.fileId = fileId;
    this.underlying = underlying;
    isEmbedded = !(underlying instanceof IndexRIDContainerSBTree);
    this.durableNonTxMode = durableNonTxMode;
  }

  public void setTopThreshold(int topThreshold) {
    this.topThreshold = topThreshold;
  }

  private static long resolveFileIdByName(String fileName) {
    final AbstractPaginatedStorage storage =
        (AbstractPaginatedStorage) DatabaseRecordThreadLocal.instance().get().getStorage();
    final AtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return atomicOperationsManager.calculateInsideComponentOperation(
        atomicOperation,
        fileName,
        (operation) -> {
          final long fileId;
          if (atomicOperation.isFileExists(fileName)) {
            fileId = atomicOperation.loadFile(fileName);
          } else {
            fileId = atomicOperation.addFile(fileName);
          }
          return fileId;
        });
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public int size() {
    return underlying.size();
  }

  @Override
  public boolean isEmpty() {
    return underlying.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return underlying.contains(o);
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return underlying.iterator();
  }

  @Override
  public Object[] toArray() {
    return underlying.toArray();
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    return underlying.toArray(a);
  }

  @Override
  public boolean add(Identifiable oIdentifiable) {
    final boolean res = underlying.add(oIdentifiable);
    checkTopThreshold();
    return res;
  }

  @Override
  public boolean remove(Object o) {
    final boolean res = underlying.remove(o);
    checkBottomThreshold();
    return res;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return underlying.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Identifiable> c) {
    final boolean res = underlying.addAll(c);
    checkTopThreshold();
    return res;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return underlying.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    final boolean res = underlying.removeAll(c);
    checkBottomThreshold();
    return res;
  }

  @Override
  public void clear() {
    if (isEmbedded) {
      underlying.clear();
    } else {
      final IndexRIDContainerSBTree tree = (IndexRIDContainerSBTree) underlying;
      tree.delete();
      underlying = new HashSet<>();
      isEmbedded = true;
    }
  }

  public boolean isEmbedded() {
    return isEmbedded;
  }

  public boolean isDurableNonTxMode() {
    return durableNonTxMode;
  }

  public Set<Identifiable> getUnderlying() {
    return underlying;
  }

  private void checkTopThreshold() {
    if (isEmbedded && topThreshold < underlying.size()) {
      convertToSbTree();
    }
  }

  private void checkBottomThreshold() {
    if (!isEmbedded && bottomThreshold > underlying.size()) {
      convertToEmbedded();
    }
  }

  private void convertToEmbedded() {
    final IndexRIDContainerSBTree tree = (IndexRIDContainerSBTree) underlying;

    final Set<Identifiable> set = new HashSet<>(tree);

    tree.delete();
    underlying = set;
    isEmbedded = true;
  }

  private void convertToSbTree() {
    final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
    final IndexRIDContainerSBTree tree =
        new IndexRIDContainerSBTree(fileId, (AbstractPaginatedStorage) db.getStorage());

    tree.addAll(underlying);

    underlying = tree;
    isEmbedded = false;
  }
}
