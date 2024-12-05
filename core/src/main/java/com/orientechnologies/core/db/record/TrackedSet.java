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

package com.orientechnologies.core.db.record;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.OSimpleMultiValueTracker;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of Set bound to a source YTRecord object to keep track of changes. This avoid to
 * call the makeDirty() by hand when the set is changed.
 */
public class TrackedSet<T> extends LinkedHashSet<T>
    implements RecordElement, OTrackedMultiValue<T, T>, Serializable {

  protected final RecordElement sourceRecord;
  private final boolean embeddedCollection;
  protected Class<?> genericClass;
  private boolean dirty = false;
  private boolean transactionDirty = false;

  private final OSimpleMultiValueTracker<T, T> tracker = new OSimpleMultiValueTracker<>(this);

  public TrackedSet(
      final RecordElement iRecord, final Collection<? extends T> iOrigin, final Class<?> cls) {
    this(iRecord);
    genericClass = cls;
    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  public TrackedSet(final RecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(TrackedSet.class);
  }

  @Override
  public RecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private T current;
      private final Iterator<T> underlying = TrackedSet.super.iterator();

      @Override
      public boolean hasNext() {
        return underlying.hasNext();
      }

      @Override
      public T next() {
        current = underlying.next();
        return current;
      }

      @Override
      public void remove() {
        underlying.remove();
        removeEvent(current);
      }
    };
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {

    boolean modified = false;
    for (T o : c) {
      if (add(o)) {
        modified = true;
      }
    }

    return modified;
  }

  public boolean add(@Nullable final T e) {
    if (super.add(e)) {
      addEvent(e);
      return true;
    }
    return false;
  }

  public boolean addInternal(final T e) {
    if (super.add(e)) {
      addOwnerToEmbeddedDoc(e);
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(final Object o) {
    if (super.remove(o)) {
      removeEvent((T) o);
      return true;
    }
    return false;
  }

  @Override
  public void clear() {
    for (final T item : this) {
      removeEvent(item);
    }
    super.clear();
  }

  protected void addEvent(T added) {
    addOwnerToEmbeddedDoc(added);

    if (tracker.isEnabled()) {
      tracker.add(added, added);
    } else {
      setDirty();
    }
  }

  private void removeEvent(T removed) {
    if (removed instanceof YTEntityImpl) {
      ODocumentInternal.removeOwner((YTEntityImpl) removed, this);
    }

    if (tracker.isEnabled()) {
      tracker.remove(removed, removed);
    } else {
      setDirty();
    }
  }

  @SuppressWarnings("unchecked")
  public TrackedSet<T> setDirty() {
    if (sourceRecord != null) {
      if (!(sourceRecord instanceof YTRecordAbstract)
          || !((YTRecordAbstract) sourceRecord).isDirty()) {
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

  public Set<T> returnOriginalState(
      YTDatabaseSessionInternal session,
      final List<OMultiValueChangeEvent<T, T>> multiValueChangeEvents) {
    final Set<T> reverted = new HashSet<T>(this);

    final ListIterator<OMultiValueChangeEvent<T, T>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<T, T> event = listIterator.previous();
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

  public Class<?> getGenericClass() {
    return genericClass;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof YTEntityImpl && !((YTEntityImpl) e).getIdentity()
        .isValid()) {
      ODocumentInternal.addOwner((YTEntityImpl) e, this);
    }

    if (e instanceof YTEntityImpl) {
      ORecordInternal.track(sourceRecord, (YTEntityImpl) e);
    }
  }

  private Object writeReplace() {
    return new HashSet<T>(this);
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.remove(event.getKey());
    super.add((T) newValue);
  }

  public void enableTracking(RecordElement parent) {
    if (!tracker.isEnabled()) {
      this.tracker.enable();
      OTrackedMultiValue.nestedEnabled(this.iterator(), this);
    }
  }

  public void disableTracking(RecordElement document) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      OTrackedMultiValue.nestedDisable(this.iterator(), this);
    }
    this.dirty = false;
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    OTrackedMultiValue.nestedTransactionClear(this.iterator());
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

  public OMultiValueChangeTimeLine<T, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
