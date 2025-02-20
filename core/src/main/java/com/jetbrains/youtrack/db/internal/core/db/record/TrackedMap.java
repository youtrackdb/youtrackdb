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
import com.jetbrains.youtrack.db.internal.core.record.impl.SimpleMultiValueTracker;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of LinkedHashMap bound to a source Record object to keep track of changes. This
 * avoid to call the makeDirty() by hand when the map is changed.
 */
public class TrackedMap<T> extends LinkedHashMap<String, T>
    implements RecordElement, TrackedMultiValue<String, T>, Serializable {

  protected RecordElement sourceRecord;
  protected Class<?> genericClass;
  private final boolean embeddedCollection;
  private boolean dirty = false;
  private boolean transactionDirty = false;

  private final SimpleMultiValueTracker<String, T> tracker = new SimpleMultiValueTracker<>(this);

  public TrackedMap(
      final RecordElement iRecord, final Map<String, T> iOrigin, final Class<?> cls) {
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

  public TrackedMap() {
    embeddedCollection = this.getClass().equals(TrackedMap.class);
  }

  public TrackedMap(int size) {
    super(size);
    embeddedCollection = this.getClass().equals(TrackedMap.class);
  }

  @Override
  public void setOwner(RecordElement owner) {
    this.sourceRecord = owner;
  }

  @Override
  public boolean isEmbeddedContainer() {
    return embeddedCollection;
  }

  @Override
  public RecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public boolean addInternal(T e) {
    throw new UnsupportedOperationException();
  }

  public void putInternal(final String key, final T value) {
    if (key == null) {
      throw new IllegalArgumentException("null key not supported by embedded map");
    }
    checkValue(value);

    var containsKey = containsKey(key);
    var oldValue = super.put(key, value);

    if (containsKey && oldValue == value) {
      return;
    }

    if (oldValue instanceof EntityImpl) {
      ((EntityImpl) oldValue).removeOwner(this);
    }

    addOwner(value);
  }

  @Override
  public T put(final String key, final T value) {
    if (key == null) {
      throw new IllegalArgumentException("null key not supported by embedded map");
    }
    checkValue(value);

    var containsKey = containsKey(key);

    var oldValue = super.put(key, value);

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

  @Override
  public T remove(final Object key) {
    var containsKey = containsKey(key);

    if (containsKey) {
      final var oldValue = super.remove(key);
      removeEvent(key.toString(), oldValue);
      return oldValue;
    } else {
      return null;
    }
  }

  private void addOwner(T e) {
    if (embeddedCollection) {
      if (e instanceof EntityImpl entity) {
        var rid = entity.getIdentity();

        if (!rid.isValid() || rid.isNew()) {
          ((EntityImpl) e).addOwner(this);
        }
      }
    } else if (e instanceof TrackedMultiValue<?, ?> trackedMultiValue) {
      trackedMultiValue.setOwner(this);
    }
  }

  private void removeOwner(T oldValue) {
    if (oldValue instanceof TrackedMultiValue<?, ?> trackedMultiValue) {
      trackedMultiValue.setOwner(null);
    } else if (oldValue instanceof EntityImpl entity) {
      entity.removeOwner(this);
    }
  }


  @Override
  public void clear() {
    for (var entry : super.entrySet()) {
      var value = entry.getValue();
      if (value instanceof TrackedMultiValue<?, ?> trackedMultiValue) {
        trackedMultiValue.setOwner(null);
      }

      removeEvent(entry.getKey(), value);
    }

    super.clear();
  }

  @Override
  public void putAll(Map<? extends String, ? extends T> m) {
    for (var entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
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

  public Map<Object, T> returnOriginalState(
      DatabaseSessionInternal session,
      final List<MultiValueChangeEvent<String, T>> multiValueChangeEvents) {
    final Map<Object, T> reverted = new HashMap<Object, T>(this);

    final var listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final var event = listIterator.previous();
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
    //noinspection unchecked
    super.put(event.getKey().toString(), (T) newValue);
  }

  private void addEvent(String key, T value) {
    addOwner(value);

    if (tracker.isEnabled()) {
      tracker.add(key, value);
    } else {
      setDirty();
    }
  }

  private void updateEvent(String key, T oldValue, T newValue) {
    removeOwner(oldValue);

    addOwner(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(key, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(String key, T removed) {
    removeOwner(removed);

    if (tracker.isEnabled()) {
      tracker.remove(key, removed);
    } else {
      setDirty();
    }
  }

  public void enableTracking(RecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
      TrackedMultiValue.nestedEnabled(this.values().iterator(), this);
    }

    this.sourceRecord = parent;
  }

  public void disableTracking(RecordElement entity) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      TrackedMultiValue.nestedDisable(this.values().iterator(), this);
    }
    this.dirty = false;
    this.sourceRecord = entity;
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    TrackedMultiValue.nestedTransactionClear(this.values().iterator());
    this.transactionDirty = false;
  }

  @Override
  public boolean isModified() {
    return dirty || tracker.isEnabled() && tracker.isChanged();
  }

  @Override
  public boolean isTransactionModified() {
    return transactionDirty || tracker.isEnabled() && tracker.isTxChanged();
  }

  @Override
  public MultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return tracker.getTimeLine();
  }

  public MultiValueChangeTimeLine<String, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
