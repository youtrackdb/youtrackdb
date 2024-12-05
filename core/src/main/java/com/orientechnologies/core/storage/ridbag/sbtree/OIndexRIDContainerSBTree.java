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

package com.orientechnologies.core.storage.ridbag.sbtree;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.core.storage.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.core.storage.index.sbtree.OTreeInternal;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Persistent Set<YTIdentifiable> implementation that uses the SBTree to handle entries in persistent
 * way.
 */
public class OIndexRIDContainerSBTree implements Set<YTIdentifiable> {

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

  private final OSBTreeBonsaiLocal<YTIdentifiable, Boolean> tree;
  private final OAtomicOperationsManager atomicOperationsManager;

  OIndexRIDContainerSBTree(long fileId, OAbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new OSBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);

    try {
      tree.create(atomicOperation, OCompactedLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTDatabaseException("Error during creation of index container "), e);
    }
  }

  public OIndexRIDContainerSBTree(
      long fileId, OBonsaiBucketPointer rootPointer, OAbstractPaginatedStorage storage) {
    String fileName;

    atomicOperationsManager = storage.getAtomicOperationsManager();
    OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(fileId);
    } else {
      fileName = atomicOperation.fileNameById(fileId);
    }

    tree =
        new OSBTreeBonsaiLocal<>(
            fileName.substring(0, fileName.length() - INDEX_FILE_EXTENSION.length()),
            INDEX_FILE_EXTENSION,
            storage);
    tree.load(rootPointer);
  }

  public OBonsaiBucketPointer getRootPointer() {
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
    return o instanceof YTIdentifiable && contains((YTIdentifiable) o);
  }

  public boolean contains(YTIdentifiable o) {
    return tree.get(o) != null;
  }

  @Override
  public Iterator<YTIdentifiable> iterator() {
    return new TreeKeyIterator(tree, false);
  }

  @Override
  public Object[] toArray() {
    final ArrayList<YTIdentifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray();
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    final ArrayList<YTIdentifiable> list = new ArrayList<>(size());

    list.addAll(this);

    return list.toArray(a);
  }

  @Override
  public boolean add(YTIdentifiable oIdentifiable) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    return this.tree.put(atomicOperation, oIdentifiable, Boolean.TRUE);
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof YTIdentifiable && remove((YTIdentifiable) o);
  }

  public boolean remove(YTIdentifiable o) {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
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
  public boolean addAll(Collection<? extends YTIdentifiable> c) {
    boolean modified = false;
    for (YTIdentifiable e : c) {
      if (add(e)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<YTIdentifiable> it = iterator();
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
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);
    tree.clear(atomicOperation);
  }

  public void delete() {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    Objects.requireNonNull(atomicOperation);

    tree.delete(atomicOperation);
  }

  public String getName() {
    return tree.getName();
  }

  private static class TreeKeyIterator implements Iterator<YTIdentifiable> {

    private final boolean autoConvertToRecord;
    private final OSBTreeMapEntryIterator<YTIdentifiable, Boolean> entryIterator;

    private TreeKeyIterator(
        OTreeInternal<YTIdentifiable, Boolean> tree, boolean autoConvertToRecord) {
      entryIterator = new OSBTreeMapEntryIterator<>(tree);
      this.autoConvertToRecord = autoConvertToRecord;
    }

    @Override
    public boolean hasNext() {
      return entryIterator.hasNext();
    }

    @Override
    public YTIdentifiable next() {
      final YTIdentifiable identifiable = entryIterator.next().getKey();
      if (autoConvertToRecord) {
        try {
          return identifiable.getRecord();
        } catch (YTRecordNotFoundException rnf) {
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
