package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OMixedIndexRIDContainer implements Set<YTIdentifiable> {

  private static final String INDEX_FILE_EXTENSION = ".irs";

  private final long fileId;
  private final Set<YTRID> embeddedSet;
  private OIndexRIDContainerSBTree tree = null;
  private final int topThreshold =
      GlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();

  /**
   * Should be called inside of lock to ensure uniqueness of entity on disk !!!
   */
  public OMixedIndexRIDContainer(String name, AtomicLong bonsayFileId) {
    long gotFileId = bonsayFileId.get();
    if (gotFileId == 0) {
      gotFileId = resolveFileIdByName(name + INDEX_FILE_EXTENSION);
      bonsayFileId.set(gotFileId);
    }
    this.fileId = gotFileId;

    embeddedSet = new HashSet<>();
  }

  public OMixedIndexRIDContainer(
      long fileId, Set<YTRID> embeddedSet, OIndexRIDContainerSBTree tree) {
    this.fileId = fileId;
    this.embeddedSet = embeddedSet;
    this.tree = tree;
  }

  private static long resolveFileIdByName(String fileName) {
    final AbstractPaginatedStorage storage =
        (AbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
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
    if (tree == null) {
      return embeddedSet.size();
    }

    return embeddedSet.size() + tree.size();
  }

  @Override
  public boolean isEmpty() {
    if (tree == null) {
      return embeddedSet.isEmpty();
    }

    return embeddedSet.isEmpty() && tree.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    if (tree == null) {
      return embeddedSet.contains(o);
    }

    return embeddedSet.contains(o) || tree.contains(o);
  }

  @Override
  public Iterator<YTIdentifiable> iterator() {
    if (tree == null) {
      return new Iterator<YTIdentifiable>() {
        private final Iterator<YTRID> embeddedIterator = embeddedSet.iterator();

        @Override
        public boolean hasNext() {
          return embeddedIterator.hasNext();
        }

        @Override
        public YTIdentifiable next() {
          return embeddedIterator.next();
        }
      };
    }

    return new Iterator<YTIdentifiable>() {
      private final Iterator<YTRID> embeddedIterator = embeddedSet.iterator();
      private final Iterator<YTIdentifiable> treeIterator = tree.iterator();

      @Override
      public boolean hasNext() {
        if (embeddedIterator.hasNext()) {
          return true;
        }

        return treeIterator.hasNext();
      }

      @Override
      public YTIdentifiable next() {
        if (embeddedIterator.hasNext()) {
          return embeddedIterator.next();
        }

        return treeIterator.next();
      }
    };
  }

  @Override
  public Object[] toArray() {
    if (tree == null) {
      return embeddedSet.toArray();
    }

    final Object[] embeddedArray = embeddedSet.toArray();
    final Object[] treeArray = tree.toArray();

    final Object[] result = new Object[embeddedArray.length + treeArray.length];
    System.arraycopy(embeddedArray, 0, result, 0, embeddedArray.length);
    System.arraycopy(treeArray, 0, result, embeddedArray.length, treeArray.length);

    return result;
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    if (tree == null) {
      return embeddedSet.toArray(a);
    }

    final T[] embeddedArray = embeddedSet.toArray(a);
    final T[] treeArray = tree.toArray(a);

    @SuppressWarnings("unchecked") final T[] result =
        (T[])
            java.lang.reflect.Array.newInstance(
                a.getClass().getComponentType(), embeddedArray.length + treeArray.length);

    System.arraycopy(embeddedArray, 0, result, 0, embeddedArray.length);
    System.arraycopy(treeArray, 0, result, embeddedArray.length, treeArray.length);

    return result;
  }

  @Override
  public boolean add(YTIdentifiable oIdentifiable) {
    if (embeddedSet.contains(oIdentifiable.getIdentity())) {
      return false;
    }

    if (embeddedSet.size() < topThreshold) {
      return embeddedSet.add(oIdentifiable.getIdentity());
    }

    if (tree == null) {
      final YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (AbstractPaginatedStorage) db.getStorage());
    }

    return tree.add(oIdentifiable);
  }

  public boolean addEntry(YTIdentifiable identifiable) {
    if (embeddedSet.contains(identifiable.getIdentity())) {
      return false;
    }

    if (embeddedSet.size() < topThreshold) {
      return embeddedSet.add(identifiable.getIdentity());
    }

    boolean treeWasCreated = false;
    if (tree == null) {
      final YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (AbstractPaginatedStorage) db.getStorage());
      treeWasCreated = true;
    }

    tree.add(identifiable);

    return treeWasCreated;
  }

  @Override
  public boolean remove(Object o) {
    boolean res = embeddedSet.remove(o);
    if (res) {
      return true;
    }

    if (tree == null) {
      return false;
    }

    return tree.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    if (embeddedSet.containsAll(c)) {
      return true;
    }

    if (tree == null) {
      return false;
    }

    final List<?> copy = new ArrayList<>(c);
    //noinspection SuspiciousMethodCalls
    copy.removeAll(embeddedSet);

    return tree.containsAll(copy);
  }

  @Override
  public boolean addAll(Collection<? extends YTIdentifiable> c) {
    final int sizeDiff = topThreshold - embeddedSet.size();
    boolean changed = false;

    final Iterator<? extends YTIdentifiable> iterator = c.iterator();
    for (int i = 0; i < sizeDiff; i++) {
      if (iterator.hasNext()) {
        final YTIdentifiable identifiable = iterator.next();
        changed = changed | embeddedSet.add(identifiable.getIdentity());
      } else {
        return changed;
      }
    }

    if (c.size() > sizeDiff) {
      if (tree == null) {
        final YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
        tree = new OIndexRIDContainerSBTree(fileId, (AbstractPaginatedStorage) db.getStorage());
      }

      while (iterator.hasNext()) {
        final YTIdentifiable identifiable = iterator.next();
        changed = changed | tree.add(identifiable);
      }
    }

    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean changed = embeddedSet.retainAll(c);
    if (tree != null) {
      changed = changed | tree.retainAll(c);
    }

    return changed;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = embeddedSet.removeAll(c);
    if (tree != null) {
      changed = changed | tree.removeAll(c);
    }

    return changed;
  }

  @Override
  public void clear() {
    embeddedSet.clear();

    if (tree != null) {
      tree.delete();
      tree = null;
    }
  }

  public void delete() {
    if (tree != null) {
      tree.delete();
      tree = null;
    } else if (fileId > 0) {
      final YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (AbstractPaginatedStorage) db.getStorage());
      tree.delete();
    }
  }

  public Set<YTRID> getEmbeddedSet() {
    return embeddedSet;
  }

  public OIndexRIDContainerSBTree getTree() {
    return tree;
  }
}
