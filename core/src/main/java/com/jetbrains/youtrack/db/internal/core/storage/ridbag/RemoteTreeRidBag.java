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

import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBagDelegate;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.SimpleMultiValueTracker;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.RecordSerializationContext;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
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

  private final SimpleMultiValueTracker<Identifiable, Identifiable> tracker =
      new SimpleMultiValueTracker<>(this);

  private transient RecordElement owner;
  private boolean dirty;
  private boolean transactionDirty = false;
  private RecordId ownerRecord;
  private String fieldName;
  private final BonsaiCollectionPointer collectionPointer;

  private class RemovableIterator implements Iterator<Identifiable> {

    private final Iterator<Identifiable> iter;
    private Identifiable next;
    private Identifiable removeNext;

    public RemovableIterator(Iterator<Identifiable> iterator) {
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
    public Identifiable next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Identifiable val = next;
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

  public RemoteTreeRidBag(BonsaiCollectionPointer pointer) {
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
          "This data structure is owned by entity "
              + owner
              + " if you want to use it in other entity create new rid bag instance and copy"
              + " content of current one.");
    }
    this.owner = owner;
    if (this.owner != null && tracker.getTimeLine() != null) {
      for (MultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            RecordInternal.track(this.owner, (Identifiable) event.getKey());
            break;
        }
      }
    }
  }

  @Override
  public Iterator<Identifiable> iterator() {
    List<Identifiable> set = loadElements();
    return new RemovableIterator(set.iterator());
  }

  private List<Identifiable> loadElements() {
    DatabaseSessionInternal database = DatabaseRecordThreadLocal.instance().get();
    List<Identifiable> set;
    try (ResultSet result =
        database.query("select list(@this.field(?)) as entities from ?", fieldName, ownerRecord)) {
      if (result.hasNext()) {
        set = (result.next().getProperty("entities"));
      } else {
        set = ((List<Identifiable>) (List) Collections.emptyList());
      }
    }
    if (tracker.getTimeLine() != null) {
      for (MultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            set.add((Identifiable) event.getKey());
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
  public void addAll(Collection<Identifiable> values) {
    for (Identifiable identifiable : values) {
      add(identifiable);
    }
  }

  @Override
  public boolean addInternal(Identifiable e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(final Identifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    if (size >= 0) {
      size++;
    }

    addEvent(identifiable, identifiable);
  }

  @Override
  public void remove(Identifiable identifiable) {
    size--;
    boolean exists;
    removeEvent(identifiable);
  }

  @Override
  public boolean contains(Identifiable identifiable) {
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
  public NavigableMap<Identifiable, Change> getChanges() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public Class<?> getGenericClass() {
    return Identifiable.class;
  }

  @Override
  public Object returnOriginalState(
      DatabaseSessionInternal session,
      List<MultiValueChangeEvent<Identifiable, Identifiable>> multiValueChangeEvents) {
    final RemoteTreeRidBag reverted = new RemoteTreeRidBag(this.collectionPointer);
    for (Identifiable identifiable : this) {
      reverted.add(identifiable);
    }

    final ListIterator<MultiValueChangeEvent<Identifiable, Identifiable>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final MultiValueChangeEvent<Identifiable, Identifiable> event = listIterator.previous();
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
    int result = 2 * LongSerializer.LONG_SIZE + 3 * IntegerSerializer.INT_SIZE;
    if (DatabaseRecordThreadLocal.instance().get().isRemote()
        || RecordSerializationContext.getContext() == null) {
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
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    // do nothing not needed
  }

  private void addEvent(Identifiable key, Identifiable identifiable) {
    if (this.owner != null) {
      RecordInternal.track(this.owner, identifiable);
    }

    if (tracker.isEnabled()) {
      tracker.addNoDirty(key, identifiable);
    } else {
      setDirtyNoChanged();
    }
  }

  private void removeEvent(Identifiable removed) {

    if (this.owner != null) {
      RecordInternal.unTrack(this.owner, removed);
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

  public void disableTracking(RecordElement entity) {
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
  public MultiValueChangeTimeLine<Object, Object> getTimeLine() {
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
  public SimpleMultiValueTracker<Identifiable, Identifiable> getTracker() {
    return tracker;
  }

  @Override
  public void setTracker(SimpleMultiValueTracker<Identifiable, Identifiable> tracker) {
    this.tracker.sourceFrom(tracker);
  }

  @Override
  public MultiValueChangeTimeLine<Identifiable, Identifiable> getTransactionTimeLine() {
    return this.tracker.getTransactionTimeLine();
  }

  public void setRecordAndField(RecordId id, String fieldName) {
    this.ownerRecord = id;
    this.fieldName = fieldName;
  }

  public BonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }
}
