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

package com.jetbrains.youtrack.db.internal.core.storage.ridbag;

import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OLongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.OMultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.OMultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBagDelegate;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.OSimpleMultiValueTracker;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

public class RemoteTreeRidBag implements RidBagDelegate {

  /**
   * Entries with not valid id.
   */
  private int size;

  private final OSimpleMultiValueTracker<YTIdentifiable, YTIdentifiable> tracker =
      new OSimpleMultiValueTracker<>(this);

  private transient RecordElement owner;
  private boolean dirty;
  private boolean transactionDirty = false;
  private YTRecordId ownerRecord;
  private String fieldName;
  private final OBonsaiCollectionPointer collectionPointer;

  private class RemovableIterator implements Iterator<YTIdentifiable> {

    private final Iterator<YTIdentifiable> iter;
    private YTIdentifiable next;
    private YTIdentifiable removeNext;

    public RemovableIterator(Iterator<YTIdentifiable> iterator) {
      this.iter = iterator;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      } else {
        if (iter.hasNext()) {
          next = iter.next();
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public YTIdentifiable next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      YTIdentifiable val = next;
      removeNext = next;
      next = null;
      return val;
    }

    @Override
    public void remove() {
      if (removeNext != null) {
        RemoteTreeRidBag.this.remove(removeNext);
        removeNext = null;
      } else {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public void setSize(int size) {
    this.size = size;
  }

  public RemoteTreeRidBag(OBonsaiCollectionPointer pointer) {
    this.size = -1;
    this.collectionPointer = pointer;
  }

  @Override
  public RecordElement getOwner() {
    return owner;
  }

  @Override
  public void setOwner(RecordElement owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException(
          "This data structure is owned by document "
              + owner
              + " if you want to use it in other document create new rid bag instance and copy"
              + " content of current one.");
    }
    this.owner = owner;
    if (this.owner != null && tracker.getTimeLine() != null) {
      for (OMultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            ORecordInternal.track(this.owner, (YTIdentifiable) event.getKey());
            break;
        }
      }
    }
  }

  @Override
  public Iterator<YTIdentifiable> iterator() {
    List<YTIdentifiable> set = loadElements();
    return new RemovableIterator(set.iterator());
  }

  private List<YTIdentifiable> loadElements() {
    YTDatabaseSessionInternal database = ODatabaseRecordThreadLocal.instance().get();
    List<YTIdentifiable> set;
    try (YTResultSet result =
        database.query("select list(@this.field(?)) as elements from ?", fieldName, ownerRecord)) {
      if (result.hasNext()) {
        set = (result.next().getProperty("elements"));
      } else {
        set = ((List<YTIdentifiable>) (List) Collections.emptyList());
      }
    }
    if (tracker.getTimeLine() != null) {
      for (OMultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            set.add((YTIdentifiable) event.getKey());
            break;
          case REMOVE:
            set.remove(event.getKey());
            break;
        }
      }
    }
    return set;
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

    if (size >= 0) {
      size++;
    }

    addEvent(identifiable, identifiable);
  }

  @Override
  public void remove(YTIdentifiable identifiable) {
    size--;
    boolean exists;
    removeEvent(identifiable);
  }

  @Override
  public boolean contains(YTIdentifiable identifiable) {
    return loadElements().contains(identifiable);
  }

  @Override
  public int size() {
    return updateSize();
  }

  @Override
  public String toString() {
    if (size >= 0) {
      return "[size=" + size + "]";
    }

    return "[...]";
  }

  @Override
  public NavigableMap<YTIdentifiable, Change> getChanges() {
    return new ConcurrentSkipListMap<>();
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
    final RemoteTreeRidBag reverted = new RemoteTreeRidBag(this.collectionPointer);
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

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void requestDelete() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    throw new UnsupportedOperationException();
  }

  /**
   * Recalculates real bag size.
   *
   * @return real size
   */
  private int updateSize() {
    this.size = loadElements().size();
    return size;
  }

  private int getChangesSerializedSize() {
    throw new UnsupportedOperationException();
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

  public void enableTracking(RecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
    }
  }

  public void disableTracking(RecordElement document) {
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

  public void setRecordAndField(YTRecordId id, String fieldName) {
    this.ownerRecord = id;
    this.fieldName = fieldName;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }
}
