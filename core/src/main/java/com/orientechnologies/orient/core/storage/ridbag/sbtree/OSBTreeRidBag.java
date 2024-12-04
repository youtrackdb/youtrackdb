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

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagDeleteSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagUpdateSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Persistent Set<YTIdentifiable> implementation that uses the SBTree to handle entries in persistent
 * way.
 */
public class OSBTreeRidBag implements ORidBagDelegate {

  private final OSBTreeCollectionManager collectionManager =
      ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
  private final ConcurrentSkipListMap<YTIdentifiable, Change> changes =
      new ConcurrentSkipListMap<>();

  /**
   * Entries with not valid id.
   */
  private final IdentityHashMap<YTIdentifiable, OModifiableInteger> newEntries =
      new IdentityHashMap<>();

  private OBonsaiCollectionPointer collectionPointer;
  private int size;

  private final OSimpleMultiValueTracker<YTIdentifiable, YTIdentifiable> tracker =
      new OSimpleMultiValueTracker<>(this);

  private transient ORecordElement owner;
  private boolean dirty;
  private boolean transactionDirty = false;

  @Override
  public void setSize(int size) {
    this.size = size;
  }

  private static class OIdentifiableIntegerEntry implements Entry<YTIdentifiable, Integer> {

    private final Entry<YTIdentifiable, Integer> entry;
    private final int newValue;

    OIdentifiableIntegerEntry(Entry<YTIdentifiable, Integer> entry, int newValue) {
      this.entry = entry;
      this.newValue = newValue;
    }

    @Override
    public YTIdentifiable getKey() {
      return entry.getKey();
    }

    @Override
    public Integer getValue() {
      return newValue;
    }

    @Override
    public Integer setValue(Integer value) {
      throw new UnsupportedOperationException();
    }
  }

  private final class RIDBagIterator implements Iterator<YTIdentifiable>, OResettable, OSizeable {

    private final NavigableMap<YTIdentifiable, Change> changedValues;
    private final SBTreeMapEntryIterator sbTreeIterator;
    private Iterator<Map.Entry<YTIdentifiable, OModifiableInteger>> newEntryIterator;
    private Iterator<Map.Entry<YTIdentifiable, Change>> changedValuesIterator;
    private Map.Entry<YTIdentifiable, Change> nextChange;
    private Map.Entry<YTIdentifiable, Integer> nextSBTreeEntry;
    private YTIdentifiable currentValue;
    private int currentFinalCounter;
    private int currentCounter;
    private boolean currentRemoved;

    private RIDBagIterator(
        IdentityHashMap<YTIdentifiable, OModifiableInteger> newEntries,
        NavigableMap<YTIdentifiable, Change> changedValues,
        SBTreeMapEntryIterator sbTreeIterator) {
      newEntryIterator = newEntries.entrySet().iterator();
      this.changedValues = changedValues;

      this.changedValuesIterator = changedValues.entrySet().iterator();
      this.sbTreeIterator = sbTreeIterator;

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null) {
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      }
    }

    @Override
    public boolean hasNext() {
      return newEntryIterator.hasNext()
          || nextChange != null
          || nextSBTreeEntry != null
          || (currentValue != null && currentCounter < currentFinalCounter);
    }

    @Override
    public YTIdentifiable next() {
      currentRemoved = false;
      if (currentCounter < currentFinalCounter) {
        currentCounter++;
        return currentValue;
      }

      if (newEntryIterator.hasNext()) {
        Map.Entry<YTIdentifiable, OModifiableInteger> entry = newEntryIterator.next();
        currentValue = entry.getKey();
        currentFinalCounter = entry.getValue().intValue();
        currentCounter = 1;
        return currentValue;
      }

      if (nextChange != null && nextSBTreeEntry != null) {
        if (nextChange.getKey().compareTo(nextSBTreeEntry.getKey()) < 0) {
          currentValue = nextChange.getKey();
          currentFinalCounter = nextChange.getValue().applyTo(0);
          currentCounter = 1;

          nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
        } else {
          currentValue = nextSBTreeEntry.getKey();
          currentFinalCounter = nextSBTreeEntry.getValue();
          currentCounter = 1;

          nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
          if (nextChange != null && nextChange.getKey().equals(currentValue)) {
            nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
          }
        }
      } else if (nextChange != null) {
        currentValue = nextChange.getKey();
        currentFinalCounter = nextChange.getValue().applyTo(0);
        currentCounter = 1;

        nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
      } else if (nextSBTreeEntry != null) {
        currentValue = nextSBTreeEntry.getKey();
        currentFinalCounter = nextSBTreeEntry.getValue();
        currentCounter = 1;

        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      } else {
        throw new NoSuchElementException();
      }

      return currentValue;
    }

    @Override
    public void remove() {
      if (currentRemoved) {
        throw new IllegalStateException("Current element has already been removed");
      }

      if (currentValue == null) {
        throw new IllegalStateException("Next method was not called for given iterator");
      }

      if (removeFromNewEntries(currentValue)) {
        if (size >= 0) {
          size--;
        }
      } else {
        Change counter = changedValues.get(currentValue);
        if (counter != null) {
          counter.decrement();
          if (size >= 0) {
            if (counter.isUndefined()) {
              size = -1;
            } else {
              size--;
            }
          }
        } else {
          if (nextChange != null) {
            changedValues.put(currentValue, new DiffChange(-1));
            changedValuesIterator =
                changedValues.tailMap(nextChange.getKey(), false).entrySet().iterator();
          } else {
            changedValues.put(currentValue, new DiffChange(-1));
          }

          size = -1;
        }
      }

      removeEvent(currentValue);
      currentRemoved = true;
    }

    @Override
    public void reset() {
      newEntryIterator = newEntries.entrySet().iterator();

      this.changedValuesIterator = changedValues.entrySet().iterator();
      if (sbTreeIterator != null) {
        this.sbTreeIterator.reset();
      }

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null) {
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      }
    }

    @Override
    public int size() {
      return OSBTreeRidBag.this.size();
    }

    private Map.Entry<YTIdentifiable, Change> nextChangedNotRemovedEntry(
        Iterator<Map.Entry<YTIdentifiable, Change>> iterator) {
      Map.Entry<YTIdentifiable, Change> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        // TODO workaround
        if (entry.getValue().applyTo(0) > 0) {
          return entry;
        }
      }

      return null;
    }
  }

  private final class SBTreeMapEntryIterator
      implements Iterator<Map.Entry<YTIdentifiable, Integer>>, OResettable {

    private final int prefetchSize;
    private LinkedList<Map.Entry<YTIdentifiable, Integer>> preFetchedValues;
    private YTIdentifiable firstKey;

    SBTreeMapEntryIterator(int prefetchSize) {
      this.prefetchSize = prefetchSize;

      init();
    }

    @Override
    public boolean hasNext() {
      return preFetchedValues != null;
    }

    @Override
    public Map.Entry<YTIdentifiable, Integer> next() {
      final Map.Entry<YTIdentifiable, Integer> entry = preFetchedValues.removeFirst();
      if (preFetchedValues.isEmpty()) {
        prefetchData(false);
      }

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      init();
    }

    private void prefetchData(boolean firstTime) {
      final OSBTreeBonsai<YTIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException(
            "RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        tree.loadEntriesMajor(
            firstKey,
            firstTime,
            true,
            entry -> {
              preFetchedValues.add(
                  new Entry<YTIdentifiable, Integer>() {
                    @Override
                    public YTIdentifiable getKey() {
                      return entry.getKey();
                    }

                    @Override
                    public Integer getValue() {
                      return entry.getValue();
                    }

                    @Override
                    public Integer setValue(Integer v) {
                      throw new UnsupportedOperationException("setValue");
                    }
                  });

              return preFetchedValues.size() <= prefetchSize;
            });
      } finally {
        releaseTree();
      }

      if (preFetchedValues.isEmpty()) {
        preFetchedValues = null;
      } else {
        firstKey = preFetchedValues.getLast().getKey();
      }
    }

    private void init() {
      OSBTreeBonsai<YTIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException(
            "RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        firstKey = tree.firstKey();
      } finally {
        releaseTree();
      }

      if (firstKey == null) {
        this.preFetchedValues = null;
        return;
      }

      this.preFetchedValues = new LinkedList<>();
      prefetchData(true);
    }
  }

  public OSBTreeRidBag(OBonsaiCollectionPointer pointer, Map<YTIdentifiable, Change> changes) {
    this.collectionPointer = pointer;
    this.changes.putAll(changes);
    this.size = -1;
  }

  public OSBTreeRidBag() {
    collectionPointer = null;
  }

  @Override
  public ORecordElement getOwner() {
    return owner;
  }

  @Override
  public void setOwner(ORecordElement owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException(
          "This data structure is owned by document "
              + owner
              + " if you want to use it in other document create new rid bag instance and copy"
              + " content of current one.");
    }
    if (this.owner != null) {
      for (YTIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
      for (YTIdentifiable entry : changes.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
    }

    this.owner = owner;
    if (this.owner != null) {
      for (YTIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
      for (YTIdentifiable entry : changes.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
    }
  }

  @Override
  public Iterator<YTIdentifiable> iterator() {
    return new RIDBagIterator(
        new IdentityHashMap<>(newEntries),
        changes,
        collectionPointer != null ? new SBTreeMapEntryIterator(1000) : null);
  }

  public boolean convertRecords2Links() {
    final Map<YTIdentifiable, Change> newChangedValues = new HashMap<>();
    for (Map.Entry<YTIdentifiable, Change> entry : changes.entrySet()) {
      newChangedValues.put(entry.getKey().getIdentity(), entry.getValue());
    }

    for (Map.Entry<YTIdentifiable, Change> entry : newChangedValues.entrySet()) {
      if (entry.getKey() instanceof YTRecord record) {

        newChangedValues.put(record, entry.getValue());
      } else {
        return false;
      }
    }

    newEntries.clear();

    changes.clear();
    changes.putAll(newChangedValues);

    return true;
  }

  public void mergeChanges(OSBTreeRidBag treeRidBag) {
    for (Map.Entry<YTIdentifiable, OModifiableInteger> entry : treeRidBag.newEntries.entrySet()) {
      mergeDiffEntry(entry.getKey(), entry.getValue().getValue());
    }

    for (Map.Entry<YTIdentifiable, Change> entry : treeRidBag.changes.entrySet()) {
      final YTIdentifiable rec = entry.getKey();
      final Change change = entry.getValue();
      final int diff;
      if (change instanceof DiffChange) {
        diff = change.getValue();
      } else if (change instanceof AbsoluteChange) {
        diff = change.getValue() - getAbsoluteValue(rec).getValue();
      } else {
        throw new IllegalArgumentException("change type is not supported");
      }

      mergeDiffEntry(rec, diff);
    }
  }

  @Override
  public void addAll(Collection<YTIdentifiable> values) {
    for (YTIdentifiable identifiable : values) {
      add(identifiable);
    }
  }

  @Override
  public boolean addInternal(YTIdentifiable e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(final YTIdentifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    if (identifiable.getIdentity().isValid()) {
      Change counter = changes.get(identifiable);
      if (counter == null) {
        changes.put(identifiable, new DiffChange(1));
      } else {
        if (counter.isUndefined()) {
          counter = getAbsoluteValue(identifiable);
          changes.put(identifiable, counter);
        }
        counter.increment();
      }
    } else {
      final OModifiableInteger counter = newEntries.get(identifiable);
      if (counter == null) {
        newEntries.put(identifiable, new OModifiableInteger(1));
      } else {
        counter.increment();
      }
    }

    if (size >= 0) {
      size++;
    }

    addEvent(identifiable, identifiable);
  }

  @Override
  public void remove(YTIdentifiable identifiable) {
    if (removeFromNewEntries(identifiable)) {
      if (size >= 0) {
        size--;
      }
    } else {
      final Change counter = changes.get(identifiable);
      if (counter == null) {
        // Not persistent keys can only be in changes or newEntries
        if (identifiable.getIdentity().isPersistent()) {
          changes.put(identifiable, new DiffChange(-1));
          size = -1;
        } else
        // Return immediately to prevent firing of event
        {
          return;
        }
      } else {
        counter.decrement();

        if (size >= 0) {
          if (counter.isUndefined()) {
            size = -1;
          } else {
            size--;
          }
        }
      }
    }

    removeEvent(identifiable);
  }

  @Override
  public boolean contains(YTIdentifiable identifiable) {
    if (newEntries.containsKey(identifiable)) {
      return true;
    }

    Change counter = changes.get(identifiable);

    if (counter != null) {
      AbsoluteChange absoluteValue = getAbsoluteValue(identifiable);

      if (counter.isUndefined()) {
        changes.put(identifiable, absoluteValue);
      }

      counter = absoluteValue;
    } else {
      counter = getAbsoluteValue(identifiable);
    }

    return counter.applyTo(0) > 0;
  }

  @Override
  public int size() {
    if (size >= 0) {
      return size;
    } else {
      return updateSize();
    }
  }

  @Override
  public String toString() {
    if (size >= 0) {
      return "[size=" + size + "]";
    }

    return "[...]";
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public Class<?> getGenericClass() {
    return YTIdentifiable.class;
  }

  @Override
  public Object returnOriginalState(
      YTDatabaseSessionInternal session,
      List<OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable>> multiValueChangeEvents) {
    final OSBTreeRidBag reverted = new OSBTreeRidBag();
    for (YTIdentifiable identifiable : this) {
      reverted.add(identifiable);
    }

    final ListIterator<OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<YTIdentifiable, YTIdentifiable> event = listIterator.previous();
      switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey());
          break;
        case REMOVE:
          reverted.add(event.getOldValue());
          break;
        default:
          throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
      }
    }

    return reverted;
  }

  @Override
  public int getSerializedSize() {
    int result = 2 * OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE;
    if (ODatabaseRecordThreadLocal.instance().get().isRemote()
        || ORecordSerializationContext.getContext() == null) {
      result += getChangesSerializedSize();
    }
    return result;
  }

  private void rearrangeChanges() {
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    for (Entry<YTIdentifiable, Change> change : this.changes.entrySet()) {
      YTIdentifiable key = change.getKey();
      if (db != null && db.getTransaction().isActive()) {
        if (!key.getIdentity().isPersistent()) {
          YTIdentifiable newKey = db.getTransaction().getRecord(key.getIdentity());
          if (newKey != null && newKey != OTransactionAbstract.DELETED_RECORD) {
            changes.remove(key);
            changes.put(newKey, change.getValue());
          }
        }
      }
    }
  }

  public void handleContextSBTree(
      ORecordSerializationContext context, OBonsaiCollectionPointer pointer) {
    rearrangeChanges();
    this.collectionPointer = pointer;
    context.push(new ORidBagUpdateSerializationOperation(changes, collectionPointer));
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    applyNewEntries();

    final ORecordSerializationContext context;

    final YTDatabaseSessionInternal databaseDocumentInternal =
        ODatabaseRecordThreadLocal.instance().get();
    var tx = databaseDocumentInternal.getTransaction();
    if (!(tx instanceof OTransactionOptimistic optimisticTx)) {
      throw new YTDatabaseException("Changes are not supported outside of transactions");
    }

    boolean remoteMode = databaseDocumentInternal.isRemote();
    if (remoteMode) {
      context = null;
    } else {
      context = ORecordSerializationContext.getContext();
    }

    // make sure that we really save underlying record.
    if (collectionPointer == null) {
      if (context != null) {
        final int clusterId = getHighLevelDocClusterId();
        assert clusterId > -1;
        try {
          final OAtomicOperationsManager atomicOperationsManager =
              ((OAbstractPaginatedStorage) databaseDocumentInternal.getStorage())
                  .getAtomicOperationsManager();
          final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          assert atomicOperation != null;
          collectionPointer =
              databaseDocumentInternal
                  .getSbTreeCollectionManager()
                  .createSBTree(clusterId, atomicOperation, ownerUuid);
        } catch (IOException e) {
          throw YTException.wrapException(new YTDatabaseException("Error during ridbag creation"),
              e);
        }
      }
    }

    OBonsaiCollectionPointer collectionPointer;
    if (this.collectionPointer != null) {
      collectionPointer = this.collectionPointer;
    } else {
      collectionPointer = OBonsaiCollectionPointer.INVALID;
    }

    OLongSerializer.INSTANCE.serializeLiteral(collectionPointer.getFileId(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    OLongSerializer.INSTANCE.serializeLiteral(rootPointer.getPageIndex(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Keep this section for binary compatibility with versions older then 1.7.5
    OIntegerSerializer.INSTANCE.serializeLiteral(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (context == null) {
      ChangeSerializationHelper.INSTANCE.serializeChanges(
          changes, OLinkSerializer.INSTANCE, stream, offset);
    } else {
      handleContextSBTree(context, collectionPointer);
      // 0-length serialized list of changes
      OIntegerSerializer.INSTANCE.serializeLiteral(0, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  public void applyNewEntries() {
    for (Entry<YTIdentifiable, OModifiableInteger> entry : newEntries.entrySet()) {
      YTIdentifiable identifiable = entry.getKey();
      assert identifiable instanceof YTRecord;
      Change c = changes.get(identifiable);

      final int delta = entry.getValue().intValue();
      if (c == null) {
        changes.put(identifiable, new DiffChange(delta));
      } else {
        c.applyDiff(delta);
      }
    }
    newEntries.clear();
  }

  public void clearChanges() {
    changes.clear();
  }

  @Override
  public void requestDelete() {
    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null && collectionPointer != null) {
      context.push(new ORidBagDeleteSerializationOperation(this));
    }
  }

  public void confirmDelete() {
    collectionPointer = null;
    changes.clear();
    newEntries.clear();
    size = 0;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    final long fileId = OLongSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long pageIndex = OLongSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Cached bag size. Not used after 1.7.5
    offset += OIntegerSerializer.INT_SIZE;

    if (fileId == -1) {
      collectionPointer = null;
    } else {
      collectionPointer =
          new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
    }

    this.size = -1;

    changes.putAll(ChangeSerializationHelper.INSTANCE.deserializeChanges(stream, offset));

    offset +=
        OIntegerSerializer.INT_SIZE + (OLinkSerializer.RID_SIZE + Change.SIZE) * changes.size();

    return offset;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  public void setCollectionPointer(OBonsaiCollectionPointer collectionPointer) {
    this.collectionPointer = collectionPointer;
  }

  private OSBTreeBonsai<YTIdentifiable, Integer> loadTree() {
    if (collectionPointer == null) {
      return null;
    }

    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    if (collectionPointer == null) {
      return;
    }

    collectionManager.releaseSBTree(collectionPointer);
  }

  private void mergeDiffEntry(YTIdentifiable key, int diff) {
    if (diff > 0) {
      for (int i = 0; i < diff; i++) {
        add(key);
      }
    } else {
      for (int i = diff; i < 0; i++) {
        remove(key);
      }
    }
  }

  private AbsoluteChange getAbsoluteValue(YTIdentifiable identifiable) {
    final OSBTreeBonsai<YTIdentifiable, Integer> tree = loadTree();
    try {
      Integer oldValue;

      if (tree == null) {
        oldValue = 0;
      } else {
        oldValue = tree.get(identifiable);
      }

      if (oldValue == null) {
        oldValue = 0;
      }

      final Change change = changes.get(identifiable);

      return new AbsoluteChange(change == null ? oldValue : change.applyTo(oldValue));
    } finally {
      releaseTree();
    }
  }

  /**
   * Recalculates real bag size.
   *
   * @return real size
   */
  private int updateSize() {
    int size = 0;
    if (collectionPointer != null) {
      final OSBTreeBonsai<YTIdentifiable, Integer> tree = loadTree();
      if (tree == null) {
        throw new IllegalStateException(
            "RidBag is not properly initialized, can not load tree implementation");
      }

      try {
        size = tree.getRealBagSize(changes);
      } finally {
        releaseTree();
      }
    } else {
      for (Change change : changes.values()) {
        size += change.applyTo(0);
      }
    }

    for (OModifiableInteger diff : newEntries.values()) {
      size += diff.getValue();
    }

    this.size = size;
    return size;
  }

  private int getChangesSerializedSize() {
    Set<YTIdentifiable> changedIds = new HashSet<>(changes.keySet());
    changedIds.addAll(newEntries.keySet());
    return ChangeSerializationHelper.INSTANCE.getChangesSerializedSize(changedIds.size());
  }

  private int getHighLevelDocClusterId() {
    ORecordElement owner = this.owner;
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null) {
      return ((YTIdentifiable) owner).getIdentity().getClusterId();
    }

    return -1;
  }

  /**
   * Removes entry with given key from {@link #newEntries}.
   *
   * @param identifiable key to remove
   * @return true if entry have been removed
   */
  private boolean removeFromNewEntries(final YTIdentifiable identifiable) {
    OModifiableInteger counter = newEntries.get(identifiable);
    if (counter == null) {
      return false;
    } else {
      if (counter.getValue() == 1) {
        newEntries.remove(identifiable);
      } else {
        counter.decrement();
      }
      return true;
    }
  }

  private Map.Entry<YTIdentifiable, Integer> nextChangedNotRemovedSBTreeEntry(
      Iterator<Map.Entry<YTIdentifiable, Integer>> iterator) {
    while (iterator.hasNext()) {
      final Map.Entry<YTIdentifiable, Integer> entry = iterator.next();
      final Change change = changes.get(entry.getKey());
      if (change == null) {
        return entry;
      }

      final int newValue = change.applyTo(entry.getValue());

      if (newValue > 0) {
        return new OIdentifiableIntegerEntry(entry, newValue);
      }
    }

    return null;
  }

  public void debugPrint(PrintStream writer) throws IOException {
    OSBTreeBonsai<YTIdentifiable, Integer> tree = loadTree();
    if (tree instanceof OSBTreeBonsaiLocal) {
      ((OSBTreeBonsaiLocal<YTIdentifiable, Integer>) tree).debugPrintBucket(writer);
    }
  }

  @Override
  public NavigableMap<YTIdentifiable, Change> getChanges() {
    applyNewEntries();
    return changes;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // do nothing not needed
  }

  private void addEvent(YTIdentifiable key, YTIdentifiable identifiable) {
    if (this.owner != null) {
      ORecordInternal.track(this.owner, identifiable);
    }

    if (tracker.isEnabled()) {
      tracker.addNoDirty(key, identifiable);
    } else {
      setDirtyNoChanged();
    }
  }

  private void removeEvent(YTIdentifiable removed) {

    if (this.owner != null) {
      ORecordInternal.unTrack(this.owner, removed);
    }

    if (tracker.isEnabled()) {
      tracker.removeNoDirty(removed, removed);
    } else {
      setDirtyNoChanged();
    }
  }

  public void enableTracking(ORecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
    }
  }

  public void disableTracking(ORecordElement document) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      this.dirty = false;
    }
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    this.transactionDirty = false;
  }

  @Override
  public boolean isModified() {
    return dirty;
  }

  @Override
  public boolean isTransactionModified() {
    return transactionDirty;
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return tracker.getTimeLine();
  }

  @Override
  public <RET> RET setDirty() {
    if (owner != null) {
      owner.setDirty();
    }
    this.dirty = true;
    this.transactionDirty = true;
    return (RET) this;
  }

  public void setTransactionModified(boolean transactionDirty) {
    this.transactionDirty = transactionDirty;
  }

  @Override
  public void setDirtyNoChanged() {
    if (owner != null) {
      owner.setDirtyNoChanged();
    }
    this.dirty = true;
    this.transactionDirty = true;
  }

  @Override
  public OSimpleMultiValueTracker<YTIdentifiable, YTIdentifiable> getTracker() {
    return tracker;
  }

  @Override
  public void setTracker(OSimpleMultiValueTracker<YTIdentifiable, YTIdentifiable> tracker) {
    this.tracker.sourceFrom(tracker);
  }

  @Override
  public OMultiValueChangeTimeLine<YTIdentifiable, YTIdentifiable> getTransactionTimeLine() {
    return this.tracker.getTransactionTimeLine();
  }
}
