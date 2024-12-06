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
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.SimpleMultiValueTracker;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Implementation of LinkedHashMap bound to a source Record object to keep track of changes. This
 * avoid to call the makeDirty() by hand when the map is changed.
 */
@SuppressWarnings("serial")
public class TrackedMap<T> extends LinkedHashMap<Object, T>
    implements RecordElement, TrackedMultiValue<Object, T>, Serializable {

  protected final RecordElement sourceRecord;
  protected Class<?> genericClass;
  private final boolean embeddedCollection;
  private boolean dirty = false;
  private boolean transactionDirty = false;

  private final SimpleMultiValueTracker<Object, T> tracker = new SimpleMultiValueTracker<>(this);

  public TrackedMap(
      final RecordElement iRecord, final Map<Object, T> iOrigin, final Class<?> cls) {
    this(iRecord);
    genericClass = cls;
    if (iOrigin != null && !iOrigin.isEmpty()) {
      putAll(iOrigin);
    }
  }

  public TrackedMap(final RecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(TrackedMap.class);
  }

  @Override
  public RecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public boolean addInternal(T e) {
    throw new UnsupportedOperationException();
  }

  public T putInternal(final Object key, final T value) {
    if (key == null) {
      throw new IllegalArgumentException("null key not supported by embedded map");
    }
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value) {
      return oldValue;
    }

    if (oldValue instanceof EntityImpl) {
      DocumentInternal.removeOwner((EntityImpl) oldValue, this);
    }

    addOwnerToEmbeddedDoc(value);

    return oldValue;
  }

  @Override
  public T put(final Object key, final T value) {
    if (key == null) {
      throw new IllegalArgumentException("null key not supported by embedded map");
    }
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value) {
      return oldValue;
    }
    if (containsKey) {
      updateEvent(key, oldValue, value);
    } else {
      addEvent(key, value);
    }
    return oldValue;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof EntityImpl && !((EntityImpl) e).getIdentity()
        .isValid()) {
      DocumentInternal.addOwner((EntityImpl) e, this);
    }
    if (e instanceof EntityImpl) {
      RecordInternal.track(sourceRecord, (EntityImpl) e);
    }
  }

  @Override
  public T remove(final Object iKey) {
    boolean containsKey = containsKey(iKey);
    if (containsKey) {
      final T oldValue = super.remove(iKey);
      removeEvent(iKey, oldValue);
      return oldValue;
    } else {
      return null;
    }
  }

  @Override
  public void clear() {
    for (Map.Entry<Object, T> entry : super.entrySet()) {
      removeEvent(entry.getKey(), entry.getValue());
    }
    super.clear();
  }

  @Override
  public void putAll(Map<?, ? extends T> m) {
    for (Map.Entry<?, ? extends T> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @SuppressWarnings({"unchecked"})
  public TrackedMap<T> setDirty() {
    if (sourceRecord != null) {
      if (!(sourceRecord instanceof RecordAbstract)
          || !((RecordAbstract) sourceRecord).isDirty()) {
        sourceRecord.setDirty();
      }
    }
    this.dirty = true;
    this.transactionDirty = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) {
      sourceRecord.setDirtyNoChanged();
    }
  }

  public Map<Object, T> returnOriginalState(
      DatabaseSessionInternal session,
      final List<MultiValueChangeEvent<Object, T>> multiValueChangeEvents) {
    final Map<Object, T> reverted = new HashMap<Object, T>(this);

    final ListIterator<MultiValueChangeEvent<Object, T>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final MultiValueChangeEvent<Object, T> event = listIterator.previous();
      switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey());
          break;
        case REMOVE:
          reverted.put(event.getKey(), event.getOldValue());
          break;
        case UPDATE:
          reverted.put(event.getKey(), event.getOldValue());
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

  private Object writeReplace() {
    return new LinkedHashMap<Object, T>(this);
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.put(event.getKey(), (T) newValue);
  }

  private void addEvent(Object key, T value) {
    addOwnerToEmbeddedDoc(value);

    if (tracker.isEnabled()) {
      tracker.add(key, value);
    } else {
      setDirty();
    }
  }

  private void updateEvent(Object key, T oldValue, T newValue) {
    if (oldValue instanceof EntityImpl) {
      DocumentInternal.removeOwner((EntityImpl) oldValue, this);
    }

    addOwnerToEmbeddedDoc(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(key, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(Object iKey, T removed) {
    if (removed instanceof EntityImpl) {
      DocumentInternal.removeOwner((EntityImpl) removed, this);
    }
    if (tracker.isEnabled()) {
      tracker.remove(iKey, removed);
    } else {
      setDirty();
    }
  }

  public void enableTracking(RecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
      TrackedMultiValue.nestedEnabled(this.values().iterator(), this);
    }
  }

  public void disableTracking(RecordElement document) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      TrackedMultiValue.nestedDisable(this.values().iterator(), this);
    }
    this.dirty = false;
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    TrackedMultiValue.nestedTransactionClear(this.values().iterator());
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

  public MultiValueChangeTimeLine<Object, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
