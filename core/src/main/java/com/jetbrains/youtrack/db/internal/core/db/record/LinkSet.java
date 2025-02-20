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

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableIdentity;
import com.jetbrains.youtrack.db.internal.core.id.IdentityChangeListener;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Lazy implementation of Set. Can be bound to a source Record object to keep track of changes.
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
public class LinkSet extends TrackedSet<Identifiable> implements IdentityChangeListener {

  public LinkSet() {
    super();
  }

  public LinkSet(int size) {
    super(size);
  }

  public LinkSet(final RecordElement iSourceRecord) {
    super(iSourceRecord);
  }

  public LinkSet(RecordElement iSourceRecord, Collection<Identifiable> iOrigin) {
    this(iSourceRecord);

    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  public boolean addInternal(final Identifiable e) {
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
      for (var item : c) {
        if (item instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(this);
        }
      }
    }

    return result;
  }

  public boolean addAll(final Collection<? extends Identifiable> c) {
    if (c == null || c.isEmpty()) {
      return false;
    }

    var result = false;
    for (var o : c) {
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
    var modified = false;
    var it = iterator();

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
    return Identifiable.class;
  }

  @Override
  public void replace(MultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }

  @Override
  @Nonnull
  public Iterator<Identifiable> iterator() {
    var iterator = super.iterator();
    return new Iterator<>() {
      private Identifiable current = null;

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Identifiable next() {
        current = iterator.next();
        return current;
      }

      @Override
      public void remove() {
        iterator.remove();

        if (current instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(LinkSet.this);
        }
        current = null;
      }
    };
  }

  @Override
  public boolean add(@Nullable Identifiable e) {
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
    super.addInternal((Identifiable) source);
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
    return RecordMultiValueHelper.toString(this);
  }

}
