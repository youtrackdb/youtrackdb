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

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.collection.LazyIterator;
import com.jetbrains.youtrack.db.internal.common.collection.LazyIteratorListWrapper;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Lazy implementation of ArrayList. It's bound to a source Record object to keep track of changes.
 * This avoid to call the makeDirty() by hand when the list is changed. It handles an internal
 * contentType to speed up some operations like conversion to/from record/links.
 */
@SuppressWarnings({"serial"})
public class LinkList extends TrackedList<Identifiable> implements Sizeable {

  protected RecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE contentType =
      MULTIVALUE_CONTENT_TYPE.EMPTY;
  protected boolean ridOnly = false;

  public LinkList() {
    super(null);
  }

  public LinkList(final RecordElement iSourceRecord) {
    super(iSourceRecord);
    if (iSourceRecord != null) {
      RecordElement source = iSourceRecord;
      while (!(source instanceof EntityImpl)) {
        source = source.getOwner();
      }
    }
  }

  public LinkList(
      final RecordElement iSourceRecord, final Collection<? extends Identifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  @Override
  public boolean addAll(Collection<? extends Identifiable> c) {
    for (Identifiable o : c) {
      add(o);
    }

    return true;
  }

  @Override
  public boolean isEmpty() {
    return super.isEmpty();
  }

  /**
   * @return iterator that just returns the elements without conversion.
   */
  public Iterator<Identifiable> rawIterator() {

    return new LazyIterator<>() {
      private int pos = -1;

      public boolean hasNext() {
        return pos < size() - 1;
      }

      public Identifiable next() {
        return LinkList.this.rawGet(++pos);
      }

      public void remove() {
        LinkList.this.remove(pos);
      }

      public Identifiable update(final Identifiable iValue) {
        return LinkList.this.set(pos, iValue);
      }
    };
  }

  public Identifiable rawGet(final int index) {
    return super.get(index);
  }

  @Override
  public LazyIterator<Identifiable> iterator() {
    return new LazyIteratorListWrapper<>(super.listIterator());
  }

  @Override
  public ListIterator<Identifiable> listIterator() {
    return super.listIterator();
  }

  @Override
  public ListIterator<Identifiable> listIterator(int index) {
    return super.listIterator(index);
  }

  @Override
  public boolean contains(final Object o) {
    return super.contains(o);
  }

  @Override
  public boolean add(Identifiable e) {
    preAdd(e);
    return super.add(e);
  }

  @Override
  public void add(int index, Identifiable e) {
    preAdd(e);
    super.add(index, e);
  }

  @Override
  public boolean addInternal(Identifiable e) {
    preAdd(e);
    return super.addInternal(e);
  }

  private void preAdd(Identifiable e) {
    if (e != null) {
      RecordInternal.track(sourceRecord, e);
      if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
          && e.getIdentity().isPersistent()
          && (e instanceof EntityImpl && !((EntityImpl) e).isDirty()))
      // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
      {
        e = e.getIdentity();
      } else {
        contentType = RecordMultiValueHelper.updateContentType(contentType, e);
      }
    }
  }

  @Override
  public Identifiable set(int index, Identifiable e) {

    if (e != null) {
      RecordInternal.track(sourceRecord, e);
      if (e != null) {
        if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
            && e.getIdentity().isPersistent()
            && (e instanceof EntityImpl && !((EntityImpl) e).isDirty()))
        // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
        {
          e = e.getIdentity();
        } else {
          contentType = RecordMultiValueHelper.updateContentType(contentType, e);
        }
      }
    }
    return super.set(index, e);
  }

  @Override
  public Identifiable get(final int index) {
    return super.get(index);
  }

  @Override
  public int indexOf(final Object o) {
    return super.indexOf(o);
  }

  @Override
  public int lastIndexOf(final Object o) {
    return super.lastIndexOf(o);
  }

  @Override
  public Identifiable remove(final int iIndex) {
    return super.remove(iIndex);
  }

  @Override
  public boolean remove(final Object iElement) {
    if (iElement == null) {
      return clearDeletedRecords();
    }
    final boolean result;
    result = super.remove(iElement);

    if (isEmpty()) {
      contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;
    }

    return result;
  }

  @Override
  public void clear() {
    super.clear();
    contentType = MULTIVALUE_CONTENT_TYPE.EMPTY;
  }

  @Override
  public int size() {
    return super.size();
  }

  @Override
  public Object[] toArray() {
    return super.toArray();
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    return super.toArray(a);
  }

  public boolean convertRecords2Links() {
    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS || sourceRecord == null)
    // PRECONDITIONS
    {
      return true;
    }

    boolean allConverted = true;
    for (int i = 0; i < super.size(); ++i) {
      try {
        if (!convertRecord2Link(i)) {
          allConverted = false;
        }
      } catch (RecordNotFoundException ignore) {
        // LEAVE THE RID DIRTY
      }
    }

    if (allConverted) {
      contentType = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;
    }

    return allConverted;
  }

  @Override
  public String toString() {
    return RecordMultiValueHelper.toString(this);
  }

  public LinkList copy(final EntityImpl iSourceRecord) {
    final LinkList copy = new LinkList(iSourceRecord);
    copy.contentType = contentType;

    final int tot = super.size();
    for (int i = 0; i < tot; ++i) {
      copy.add(rawGet(i));
    }

    return copy;
  }

  public boolean detach() {
    return convertRecords2Links();
  }

  /**
   * Convert the item requested from record to link.
   *
   * @param iIndex Position of the item to convert
   * @return <code>true</code> if conversion was successful.
   */
  private boolean convertRecord2Link(final int iIndex) {
    if (contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
    // PRECONDITIONS
    {
      return true;
    }

    final Identifiable o = super.get(iIndex);
    if (o instanceof Identifiable && o.getIdentity().isPersistent()) {
      // ALREADY CONVERTED
      if (o instanceof DBRecord && !((DBRecord) o).isDirty()) {
        try {
          super.setInternal(iIndex, o.getIdentity());
          // CONVERTED
          return true;
        } catch (RecordNotFoundException ignore) {
          // IGNORE THIS
        }
      } else {
        return o instanceof RID;
      }
    }
    return false;
  }

  public boolean clearDeletedRecords() {
    var db = getOwnerRecord().getSession();

    boolean removed = false;
    Iterator<Identifiable> it = super.iterator();
    while (it.hasNext()) {
      Identifiable rec = it.next();
      if (!db.exists(rec.getIdentity())) {
        it.remove();
        removed = true;
      }
    }
    return removed;
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }
}
