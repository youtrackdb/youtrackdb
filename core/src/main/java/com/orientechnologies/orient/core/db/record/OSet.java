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

import com.orientechnologies.orient.core.id.ChangeableIdentity;
import com.orientechnologies.orient.core.id.IdentityChangeListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Lazy implementation of Set. Can be bound to a source YTRecord object to keep track of changes.
 * This avoid to call the makeDirty() by hand when the set is changed.
 *
 * <p><b>Internals</b>:
 *
 * <ul>
 *   <li>stores new records in a separate IdentityHashMap to keep underlying list (delegate) always
 *       ordered and minimizing sort operations
 *   <li>
 * </ul>
 *
 * <p>
 */
public class OSet extends OTrackedSet<YTIdentifiable> implements IdentityChangeListener {

  public OSet(final ORecordElement iSourceRecord) {
    super(iSourceRecord);
  }

  public OSet(ORecordElement iSourceRecord, Collection<YTIdentifiable> iOrigin) {
    this(iSourceRecord);

    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  public boolean addInternal(final YTIdentifiable e) {
    var result = super.addInternal(e);

    if (result) {
      var rid = e.getIdentity();

      if (rid instanceof ChangeableIdentity changeableIdentity
          && changeableIdentity.canChangeIdentity()) {
        changeableIdentity.addIdentityChangeListener(this);
      }
    }

    return result;
  }


  public void clear() {
    for (var identifiable : this) {
      if (identifiable instanceof ChangeableIdentity changeableIdentity) {
        changeableIdentity.removeIdentityChangeListener(this);
      }
    }

    super.clear();
  }

  public boolean removeAll(final Collection<?> c) {
    var result = super.removeAll(c);

    if (result) {
      for (Object item : c) {
        if (item instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(this);
        }
      }
    }

    return result;
  }

  public boolean addAll(final Collection<? extends YTIdentifiable> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }

    var result = false;
    for (YTIdentifiable o : c) {
      var resultAdd = super.add(o);
      result = result || resultAdd;

      if (resultAdd) {
        if (o instanceof ChangeableIdentity changeableIdentity
            && changeableIdentity.canChangeIdentity()) {
          changeableIdentity.addIdentityChangeListener(this);
        }
      }
    }

    return result;
  }

  public boolean retainAll(@Nonnull final Collection<?> c) {
    if (c.isEmpty()) {
      return false;
    }

    Objects.requireNonNull(c);
    boolean modified = false;
    Iterator<YTIdentifiable> it = iterator();

    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public Class<?> getGenericClass() {
    return YTIdentifiable.class;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }

  @Override
  @Nonnull
  public Iterator<YTIdentifiable> iterator() {
    var iterator = super.iterator();
    return new Iterator<>() {
      private YTIdentifiable current = null;

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public YTIdentifiable next() {
        current = iterator.next();
        return current;
      }

      @Override
      public void remove() {
        iterator.remove();

        if (current instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(OSet.this);
        }
        current = null;
      }
    };
  }

  @Override
  public boolean add(@Nullable YTIdentifiable e) {
    var result = super.add(e);

    if (result) {
      if (e instanceof ChangeableIdentity changeableIdentity
          && changeableIdentity.canChangeIdentity()) {
        changeableIdentity.addIdentityChangeListener(this);
      }
    }

    return result;
  }

  @Override
  public void onBeforeIdentityChange(Object source) {
    super.remove(source);
  }

  @Override
  public void onAfterIdentityChange(Object source) {
    super.addInternal((YTIdentifiable) source);
  }

  public boolean remove(Object o) {
    if (o == null) {
      return false;
    }

    var result = super.remove(o);
    if (result) {
      if (o instanceof ChangeableIdentity changeableIdentity) {
        changeableIdentity.removeIdentityChangeListener(this);
      }
    }

    return result;
  }

  @Override
  public String toString() {
    return ORecordMultiValueHelper.toString(this);
  }

}
