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
package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.record.impl.SimpleMultiValueTracker;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implementation of ArrayList bound to a source Record object to keep track of changes for literal
 * types. This avoids to call the makeDirty() by hand when the list is changed.
 */
public class TrackedList<T> extends ArrayList<T>
    implements RecordElement, TrackedMultiValue<Integer, T>, Serializable {

  protected final RecordElement sourceRecord;
  protected Class<?> genericClass;
  private final boolean embeddedCollection;
  private boolean dirty = false;
  private boolean transactionDirty = false;
  private final SimpleMultiValueTracker<Integer, T> tracker = new SimpleMultiValueTracker<>(this);

  public TrackedList(
      final RecordElement iRecord,
      final Collection<? extends T> iOrigin,
      final Class<?> iGenericClass) {
    this(iRecord);
    genericClass = iGenericClass;

    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  public TrackedList(final RecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(TrackedList.class);
  }

  @Override
  public RecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public boolean add(T element) {
    final var result = super.add(element);

    if (result) {
      addEvent(super.size() - 1, element);
    }

    return result;
  }

  public boolean addInternal(T element) {
    final var result = super.add(element);

    if (result) {
      addOwnerToEmbeddedDoc(element);
    }

    return result;
  }

  @Override
  public boolean addAll(final Collection<? extends T> c) {
    for (var o : c) {
      add(o);
    }
    return true;
  }

  @Override
  public void add(int index, T element) {
    super.add(index, element);
    addEvent(index, element);
  }

  public void setInternal(int index, T element) {
    final var oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      if (oldValue instanceof EntityImpl) {
        EntityInternalUtils.removeOwner((EntityImpl) oldValue, this);
      }

      addOwnerToEmbeddedDoc(element);
    }
  }

  @Override
  public T set(int index, T element) {
    final var oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      updateEvent(index, oldValue, element);
    }

    return oldValue;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof EntityImpl entity) {
      var rid = entity.getIdentity();

      if (!rid.isValid() || rid.isNew()) {
        EntityInternalUtils.addOwner((EntityImpl) e, this);
      }
    }
  }

  @Override
  public T remove(int index) {
    final var oldValue = super.remove(index);
    removeEvent(index, oldValue);
    return oldValue;
  }

  private void addEvent(int index, T added) {
    addOwnerToEmbeddedDoc(added);

    if (tracker.isEnabled()) {
      tracker.add(index, added);
    } else {
      setDirty();
    }
  }

  private void updateEvent(int index, T oldValue, T newValue) {
    if (oldValue instanceof EntityImpl) {
      EntityInternalUtils.removeOwner((EntityImpl) oldValue, this);
    }

    addOwnerToEmbeddedDoc(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(index, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(int index, T removed) {
    if (removed instanceof EntityImpl) {
      EntityInternalUtils.removeOwner((EntityImpl) removed, this);
    }
    if (tracker.isEnabled()) {
      tracker.remove(index, removed);
    } else {
      setDirty();
    }
  }

  @Override
  public boolean remove(Object o) {
    final var index = indexOf(o);
    if (index >= 0) {
      remove(index);
      return true;
    }
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    var removed = false;
    for (var o : c) {
      removed = removed | remove(o);
    }

    return removed;
  }

  @Override
  public void clear() {
    for (var i = this.size() - 1; i >= 0; i--) {
      final var origValue = this.get(i);
      removeEvent(i, origValue);
    }
    super.clear();
  }

  public void reset() {
    super.clear();
  }

  public void setDirty() {
    this.dirty = true;
    this.transactionDirty = true;

    if (sourceRecord != null) {
      if (!(sourceRecord instanceof RecordAbstract)
          || !((RecordAbstract) sourceRecord).isDirty()) {
        sourceRecord.setDirty();
      }
    }
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) {
      sourceRecord.setDirtyNoChanged();
    }
  }

  public List<T> returnOriginalState(
      DatabaseSessionInternal session,
      final List<MultiValueChangeEvent<Integer, T>> multiValueChangeEvents) {
    final List<T> reverted = new ArrayList<T>(this);

    final var listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final var event = listIterator.previous();
      switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey().intValue());
          break;
        case REMOVE:
          reverted.add(event.getKey(), event.getOldValue());
          break;
        case UPDATE:
          reverted.set(event.getKey(), event.getOldValue());
          break;
        default:
          throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
      }
    }

    return reverted;
  }

  public Class<?> getGenericClass() {
    return genericClass;
  }

  @Serial
  private Object writeReplace() {
    return new ArrayList<>(this);
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.set((Integer) event.getKey(), (T) newValue);
  }

  public void enableTracking(RecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
      TrackedMultiValue.nestedEnabled(this.iterator(), this);
    }
  }

  public void disableTracking(RecordElement parent) {
    if (tracker.isEnabled()) {
      tracker.disable();
      TrackedMultiValue.nestedDisable(this.iterator(), this);
    }
    this.dirty = false;
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    TrackedMultiValue.nestedTransactionClear(this.iterator());
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

  public MultiValueChangeTimeLine<Integer, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
