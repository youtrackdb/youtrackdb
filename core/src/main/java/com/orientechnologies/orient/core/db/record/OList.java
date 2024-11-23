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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.collection.OLazyIteratorListWrapper;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes.
 * This avoid to call the makeDirty() by hand when the list is changed. It handles an internal
 * contentType to speed up some operations like conversion to/from record/links.
 */
@SuppressWarnings({"serial"})
public class OList extends OTrackedList<OIdentifiable> implements OSizeable {

  protected ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE contentType =
      MULTIVALUE_CONTENT_TYPE.EMPTY;
  protected boolean ridOnly = false;

  public OList() {
    super(null);
  }

  public OList(final ORecordElement iSourceRecord) {
    super(iSourceRecord);
    if (iSourceRecord != null) {
      ORecordElement source = iSourceRecord;
      while (!(source instanceof ODocument)) {
        source = source.getOwner();
      }
    }
  }

  public OList(
      final ORecordElement iSourceRecord, final Collection<? extends OIdentifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    final Iterator it = c.iterator();

    while (it.hasNext()) {
      Object o = it.next();
      if (o == null) {
        add(null);
      } else if (o instanceof OIdentifiable) {
        add((OIdentifiable) o);
      } else {
        com.orientechnologies.common.collection.OMultiValue.add(this, o);
      }
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
  public Iterator<OIdentifiable> rawIterator() {

    return new OLazyIterator<>() {
      private int pos = -1;

      public boolean hasNext() {
        return pos < size() - 1;
      }

      public OIdentifiable next() {
        return OList.this.rawGet(++pos);
      }

      public void remove() {
        OList.this.remove(pos);
      }

      public OIdentifiable update(final OIdentifiable iValue) {
        return OList.this.set(pos, iValue);
      }
    };
  }

  public OIdentifiable rawGet(final int index) {
    return super.get(index);
  }

  @Override
  public OLazyIterator<OIdentifiable> iterator() {
    return new OLazyIteratorListWrapper<>(super.listIterator());
  }

  @Override
  public ListIterator<OIdentifiable> listIterator() {
    return super.listIterator();
  }

  @Override
  public ListIterator<OIdentifiable> listIterator(int index) {
    return super.listIterator(index);
  }

  @Override
  public boolean contains(final Object o) {
    return super.contains(o);
  }

  @Override
  public boolean add(OIdentifiable e) {
    preAdd(e);
    return super.add(e);
  }

  @Override
  public void add(int index, OIdentifiable e) {
    preAdd(e);
    super.add(index, e);
  }

  @Override
  public boolean addInternal(OIdentifiable e) {
    preAdd(e);
    return super.addInternal(e);
  }

  private void preAdd(OIdentifiable e) {
    if (e != null) {
      ORecordInternal.track(sourceRecord, e);
      if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
          && e.getIdentity().isPersistent()
          && (e instanceof ODocument && !((ODocument) e).isDirty()))
      // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
      {
        e = e.getIdentity();
      } else {
        contentType = ORecordMultiValueHelper.updateContentType(contentType, e);
      }
    }
  }

  @Override
  public OIdentifiable set(int index, OIdentifiable e) {

    if (e != null) {
      ORecordInternal.track(sourceRecord, e);
      if (e != null) {
        if ((ridOnly || contentType == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
            && e.getIdentity().isPersistent()
            && (e instanceof ODocument && !((ODocument) e).isDirty()))
        // IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
        {
          e = e.getIdentity();
        } else {
          contentType = ORecordMultiValueHelper.updateContentType(contentType, e);
        }
      }
    }
    return super.set(index, e);
  }

  @Override
  public OIdentifiable get(final int index) {
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
  public OIdentifiable remove(final int iIndex) {
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
      } catch (ORecordNotFoundException ignore) {
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
    return ORecordMultiValueHelper.toString(this);
  }

  public OList copy(final ODocument iSourceRecord) {
    final OList copy = new OList(iSourceRecord);
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

    final OIdentifiable o = super.get(iIndex);
    if (o instanceof OIdentifiable && o.getIdentity().isPersistent()) {
      // ALREADY CONVERTED
      if (o instanceof ORecord && !((ORecord) o).isDirty()) {
        try {
          super.setInternal(iIndex, o.getIdentity());
          // CONVERTED
          return true;
        } catch (ORecordNotFoundException ignore) {
          // IGNORE THIS
        }
      } else {
        return o instanceof ORID;
      }
    }
    return false;
  }

  public boolean clearDeletedRecords() {
    var db = getOwnerRecord().getSession();

    boolean removed = false;
    Iterator<OIdentifiable> it = super.iterator();
    while (it.hasNext()) {
      OIdentifiable rec = it.next();
      if (!db.exists(rec.getIdentity())) {
        it.remove();
        removed = true;
      }
    }
    return removed;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }
}
