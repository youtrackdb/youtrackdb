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
package com.jetbrains.youtrack.db.internal.core.db.record.ridbag.embedded;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.common.util.Resettable;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBagDelegate;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.SimpleMultiValueTracker;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionAbstract;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.UUID;

public class EmbeddedRidBag implements RidBagDelegate {

  private boolean contentWasChanged = false;

  private Object[] entries = CommonConst.EMPTY_OBJECT_ARRAY;
  private int entriesLength = 0;

  private int size = 0;

  private transient RecordElement owner;

  private boolean dirty = false;
  private boolean transactionDirty = false;

  private SimpleMultiValueTracker<Identifiable, Identifiable> tracker =
      new SimpleMultiValueTracker<>(this);

  @Override
  public void setSize(int size) {
    this.size = size;
  }

  private enum Tombstone {
    TOMBSTONE
  }

  public Object[] getEntries() {
    return entries;
  }

  private final class EntriesIterator implements Iterator<Identifiable>, Resettable, Sizeable {

    private int currentIndex = -1;
    private int nextIndex = -1;
    private boolean currentRemoved;

    private EntriesIterator() {
      reset();
    }

    @Override
    public boolean hasNext() {
      // we may remove items in ridbag during iteration so we need to be sure that pointed item is
      // not removed.
      if (nextIndex > -1) {
        if (entries[nextIndex] instanceof Identifiable) {
          return true;
        }

        nextIndex = nextIndex();
      }

      return nextIndex > -1;
    }

    @Override
    public Identifiable next() {
      currentRemoved = false;

      currentIndex = nextIndex;
      if (currentIndex == -1) {
        throw new NoSuchElementException();
      }

      Object nextValue = entries[currentIndex];

      // we may remove items in ridbag during iteration so we need to be sure that pointed item is
      // not removed.
      if (!(nextValue instanceof Identifiable)) {
        nextIndex = nextIndex();

        currentIndex = nextIndex;
        if (currentIndex == -1) {
          throw new NoSuchElementException();
        }

        nextValue = entries[currentIndex];
      }

      if (nextValue != null) {
        if (((Identifiable) nextValue).getIdentity().isPersistent()) {
          entries[currentIndex] = ((Identifiable) nextValue).getIdentity();
        }
      }

      nextIndex = nextIndex();

      assert nextValue != null;
      return (Identifiable) nextValue;
    }

    @Override
    public void remove() {
      if (currentRemoved) {
        throw new IllegalStateException("Current entity has already been removed");
      }

      if (currentIndex == -1) {
        throw new IllegalStateException("Next method was not called for given iterator");
      }

      currentRemoved = true;

      final Identifiable nextValue = (Identifiable) entries[currentIndex];
      entries[currentIndex] = Tombstone.TOMBSTONE;

      size--;
      contentWasChanged = true;
      removeEvent(nextValue);
    }

    private void swapValueOnCurrent(Identifiable newValue) {
      if (currentRemoved) {
        throw new IllegalStateException("Current entity has already been removed");
      }

      if (currentIndex == -1) {
        throw new IllegalStateException("Next method was not called for given iterator");
      }

      final Identifiable oldValue = (Identifiable) entries[currentIndex];
      entries[currentIndex] = newValue;

      contentWasChanged = true;

      updateEvent(oldValue, oldValue, newValue);
    }

    @Override
    public void reset() {
      currentIndex = -1;
      nextIndex = -1;
      currentRemoved = false;

      nextIndex = nextIndex();
    }

    @Override
    public int size() {
      return size;
    }

    private int nextIndex() {
      for (int i = currentIndex + 1; i < entriesLength; i++) {
        Object entry = entries[i];
        if (entry instanceof Identifiable) {
          return i;
        }
      }

      return -1;
    }
  }

  @Override
  public RecordElement getOwner() {
    return owner;
  }

  @Override
  public boolean contains(Identifiable identifiable) {
    if (identifiable == null) {
      return false;
    }

    for (int i = 0; i < entriesLength; i++) {
      if (identifiable.equals(entries[i])) {
        return true;
      }
    }

    return false;
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
    if (this.owner != null) {
      for (int i = 0; i < entriesLength; i++) {
        final Object entry = entries[i];
        if (entry instanceof Identifiable) {
          RecordInternal.unTrack(this.owner, (Identifiable) entry);
        }
      }
    }

    this.owner = owner;
    if (this.owner != null) {
      for (int i = 0; i < entriesLength; i++) {
        final Object entry = entries[i];
        if (entry instanceof Identifiable) {
          RecordInternal.track(this.owner, (Identifiable) entry);
        }
      }
    }
  }

  @Override
  public void addAll(Collection<Identifiable> values) {
    for (Identifiable value : values) {
      add(value);
    }
  }

  @Override
  public void add(final Identifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }
    addEntry(identifiable);

    size++;
    contentWasChanged = true;

    addEvent(identifiable, identifiable);
  }

  public EmbeddedRidBag copy() {
    final EmbeddedRidBag copy = new EmbeddedRidBag();
    copy.contentWasChanged = contentWasChanged;
    copy.entries = entries;
    copy.entriesLength = entriesLength;
    copy.size = size;
    copy.owner = owner;
    copy.tracker = this.tracker;
    return copy;
  }

  @Override
  public void remove(Identifiable identifiable) {

    if (removeEntry(identifiable)) {
      size--;
      contentWasChanged = true;

      removeEvent(identifiable);
    }
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return new EntriesIterator();
  }

  public boolean convertRecords2Links() {
    for (int i = 0; i < entriesLength; i++) {
      final Object entry = entries[i];

      if (entry instanceof Identifiable identifiable) {
        if (identifiable instanceof Record record) {
          entries[i] = record.getIdentity();
        }
      }
    }

    return true;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    if (size < 10) {
      final StringBuilder sb = new StringBuilder(256);
      sb.append('[');
      for (final Iterator<Identifiable> it = this.iterator(); it.hasNext(); ) {
        try {
          Identifiable e = it.next();
          if (e != null) {
            if (sb.length() > 1) {
              sb.append(", ");
            }

            sb.append(e.getIdentity());
          }
        } catch (NoSuchElementException ignore) {
          // IGNORE THIS
        }
      }
      return sb.append(']').toString();

    } else {
      return "[size=" + size + "]";
    }
  }

  @Override
  public Object returnOriginalState(
      DatabaseSessionInternal session,
      List<MultiValueChangeEvent<Identifiable, Identifiable>> multiValueChangeEvents) {
    final EmbeddedRidBag reverted = new EmbeddedRidBag();
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
    int size;

    size = IntegerSerializer.INT_SIZE;

    size += this.size * LinkSerializer.RID_SIZE;

    return size;
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    IntegerSerializer.INSTANCE.serializeLiteral(size, stream, offset);
    offset += IntegerSerializer.INT_SIZE;
    DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().getIfDefined();
    final int totEntries = entries.length;
    for (int i = 0; i < totEntries; ++i) {
      final Object entry = entries[i];
      if (entry instanceof Identifiable link) {
        final RID rid = link.getIdentity();
        if (db != null && !db.isClosed() && db.getTransaction().isActive()) {
          if (!link.getIdentity().isPersistent()) {
            link = db.getTransaction().getRecord(link.getIdentity());
            if (link == FrontendTransactionAbstract.DELETED_RECORD) {
              link = null;
            }
          }
        }

        if (link == null) {
          throw new SerializationException("Found null entry in ridbag with rid=" + rid);
        }

        entries[i] = link.getIdentity();
        LinkSerializer.INSTANCE.serialize(link, stream, offset);
        offset += LinkSerializer.RID_SIZE;
      }
    }

    return offset;
  }

  @Override
  public int deserialize(final byte[] stream, int offset) {
    this.size = IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    int entriesSize = IntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += IntegerSerializer.INT_SIZE;

    for (int i = 0; i < entriesSize; i++) {
      RID rid = LinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += LinkSerializer.RID_SIZE;

      Identifiable identifiable;
      if (rid.isTemporary()) {
        try {
          identifiable = rid.getRecord();
        } catch (RecordNotFoundException rnf) {
          LogManager.instance()
              .warn(this, "Found null reference during ridbag deserialization (rid=%s)", rid);
          identifiable = rid;
        }
      } else {
        identifiable = rid;
      }

      addInternal(identifiable);
    }

    return offset;
  }

  @Override
  public void requestDelete() {
  }

  @Override
  public Class<?> getGenericClass() {
    return Identifiable.class;
  }

  public boolean addInternal(final Identifiable identifiable) {
    addEntry(identifiable);
    if (this.owner != null) {
      RecordInternal.track(this.owner, identifiable);
    }
    return true;
  }

  public void addEntry(final Identifiable identifiable) {
    if (entries.length == entriesLength) {
      if (entriesLength == 0) {
        final int cfgValue =
            GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
        entries = new Object[cfgValue > 0 ? Math.min(cfgValue, 40) : 40];
      } else {
        final Object[] oldEntries = entries;
        entries = new Object[entries.length << 1];
        System.arraycopy(oldEntries, 0, entries, 0, oldEntries.length);
      }
    }
    entries[entriesLength] = identifiable;
    entriesLength++;
  }

  private boolean removeEntry(Identifiable identifiable) {
    int i = 0;
    for (; i < entriesLength; i++) {
      final Object entry = entries[i];
      if (entry.equals(identifiable)) {
        entries[i] = Tombstone.TOMBSTONE;
        break;
      }
    }

    return i < entriesLength;
  }

  @Override
  public NavigableMap<Identifiable, Change> getChanges() {
    return null;
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    // do nothing not needed
  }

  private void addEvent(final Identifiable key, final Identifiable identifiable) {
    if (this.owner != null) {
      RecordInternal.track(this.owner, identifiable);
    }

    if (tracker.isEnabled()) {
      tracker.add(key, identifiable);
    } else {
      setDirty();
    }
  }

  private void updateEvent(Identifiable key, Identifiable oldValue, Identifiable newValue) {
    if (this.owner != null) {
      RecordInternal.unTrack(this.owner, oldValue);
    }

    if (tracker.isEnabled()) {
      tracker.updated(key, oldValue, newValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(Identifiable removed) {
    if (this.owner != null) {
      RecordInternal.unTrack(this.owner, removed);
    }

    if (tracker.isEnabled()) {
      tracker.remove(removed, removed);
    } else {
      setDirty();
    }
  }

  public void enableTracking(final RecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
    }
  }

  public void disableTracking(final RecordElement entity) {
    if (tracker.isEnabled()) {
      tracker.disable();
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
    return tracker.getTransactionTimeLine();
  }
}
