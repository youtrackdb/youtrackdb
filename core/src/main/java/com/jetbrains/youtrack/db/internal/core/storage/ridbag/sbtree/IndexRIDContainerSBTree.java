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
import com.jetbrains.youtrack.db.internal.common.serialization.types.BooleanSerializer;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.CompactedLinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.SBTreeMapEntryIterator;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.TreeInternal;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsaiLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Persistent Set<Identifiable> implementation that uses the SBTree to handle entries in persistent
 * way.
 */
public class IndexRIDContainerSBTree implements Set<Identifiable> {

  public static final String INDEX_FILE_EXTENSION = ".irs";

  /**
   * Generates a lock name for the given index name.
   *
   * @param indexName the index name to generate the lock name for.
   * @return the generated lock name.
   */
  public static String generateLockName(String indexName) {
    return indexName + INDEX_FILE_EXTENSION;
  }

  private final SBTreeBonsaiLocal<Identifiable, Boolean> tree;
  private final AtomicOperationsManager atomicOperationsManager;

  IndexRIDContainerSBTree(long fileId, AbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new SBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);

    try {
      tree.create(atomicOperation, CompactedLinkSerializer.INSTANCE, BooleanSerializer.INSTANCE);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new DatabaseException("Error during creation of index container "), e);
    }
  }

  public IndexRIDContainerSBTree(
      long fileId, BonsaiBucketPointer rootPointer, AbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new SBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);
    tree.load(rootPointer);
  }

  public BonsaiBucketPointer getRootPointer() {
    return tree.getRootBucketPointer();
  }

  @Override
  public int size() {
    return (int) tree.size();
  }

  @Override
  public boolean isEmpty() {
    return tree.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof Identifiable && contains((Identifiable) o);
  }

  public boolean contains(Identifiable o) {
    return tree.get(o) != null;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return new TreeKeyIterator(tree, false);
  }

  @Override
  public Object[] toArray() {
    final ArrayList<Identifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray();
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    final ArrayList<Identifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray(a);
  }

  @Override
  public boolean add(Identifiable oIdentifiable) {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return this.tree.put(atomicOperation, oIdentifiable, Boolean.TRUE);
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof Identifiable && remove((Identifiable) o);
  }

  public boolean remove(Identifiable o) {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return tree.remove(atomicOperation, o) != null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object e : c) {
      if (!contains(e)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends Identifiable> c) {
    boolean modified = false;
    for (Identifiable e : c) {
      if (add(e)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<Identifiable> it = iterator();
    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;
    for (Object o : c) {
      modified |= remove(o);
    }

    return modified;
  }

  @Override
  public void clear() {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);
    tree.clear(atomicOperation);
  }

  public void delete() {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    tree.delete(atomicOperation);
  }

  public String getName() {
    return tree.getName();
  }

  private static class TreeKeyIterator implements Iterator<Identifiable> {

    private final boolean autoConvertToRecord;
    private final SBTreeMapEntryIterator<Identifiable, Boolean> entryIterator;

    private TreeKeyIterator(
        TreeInternal<Identifiable, Boolean> tree, boolean autoConvertToRecord) {
      entryIterator = new SBTreeMapEntryIterator<>(tree);
      this.autoConvertToRecord = autoConvertToRecord;
    }

    @Override
    public boolean hasNext() {
      return entryIterator.hasNext();
    }

    @Override
    public Identifiable next() {
      final Identifiable identifiable = entryIterator.next().getKey();
      if (autoConvertToRecord) {
        try {
          return identifiable.getRecord();
        } catch (RecordNotFoundException rnf) {
          return identifiable;
        }
      } else {
        return identifiable;
      }
    }

    @Override
    public void remove() {
      entryIterator.remove();
    }
  }
}
