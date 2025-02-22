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
package com.jetbrains.youtrack.db.internal.common.collection;

import com.jetbrains.youtrack.db.internal.common.util.Resettable;
import com.jetbrains.youtrack.db.internal.common.util.Sizeable;
import com.jetbrains.youtrack.db.internal.common.util.SupportsContains;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.iterator.LazyWrapperIterator;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator that allow to iterate against multiple collection of elements.
 */
public class MultiCollectionIterator<T>
    implements Iterator<T>, Iterable<T>, Resettable, Sizeable, SupportsContains {

  private List<Object> sources;
  private Iterator<?> sourcesIterator;
  private Iterator<T> partialIterator;

  private int browsed = 0;
  private int skip = -1;
  private int limit = -1;
  private boolean embedded = false;
  private boolean autoConvert2Record = true;

  private int skipped = 0;

  public MultiCollectionIterator() {
    sources = new ArrayList<Object>();
  }

  public MultiCollectionIterator(final Iterator<? extends Collection<?>> iterator) {
    sourcesIterator = iterator;
    getNextPartial();
  }

  @Override
  public boolean hasNext() {
    while (skipped < skip) {
      if (!hasNextInternal()) {
        return false;
      }
      partialIterator.next();
      skipped++;
    }
    return hasNextInternal();
  }

  private boolean hasNextInternal() {
    if (sourcesIterator == null) {
      if (sources == null || sources.isEmpty()) {
        return false;
      }

      // THE FIRST TIME CREATE THE ITERATOR
      sourcesIterator = sources.iterator();
      getNextPartial();
    }

    if (partialIterator == null) {
      return false;
    }

    if (limit > -1 && browsed >= limit) {
      return false;
    }

    if (partialIterator.hasNext()) {
      return true;
    } else if (sourcesIterator.hasNext()) {
      return getNextPartial();
    }

    return false;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    browsed++;
    return partialIterator.next();
  }

  @Override
  public Iterator<T> iterator() {
    reset();
    return this;
  }

  @Override
  public void reset() {
    sourcesIterator = null;
    partialIterator = null;
    browsed = 0;
    skipped = 0;
  }

  public MultiCollectionIterator<T> add(final Object iValue) {
    if (iValue != null) {
      if (sourcesIterator != null) {
        throw new IllegalStateException(
            "MultiCollection iterator is in use and new collections cannot be added");
      }

      sources.add(iValue);
    }
    return this;
  }

  public int size() {
    // SUM ALL THE COLLECTION SIZES
    int size = 0;
    final int totSources = sources.size();
    for (int i = 0; i < totSources; ++i) {
      final Object o = sources.get(i);

      if (o != null) {
        if (o instanceof Collection<?>) {
          size += ((Collection<?>) o).size();
        } else if (o instanceof Map<?, ?>) {
          size += ((Map<?, ?>) o).size();
        } else if (o instanceof Sizeable) {
          size += ((Sizeable) o).size();
        } else if (o.getClass().isArray()) {
          size += Array.getLength(o);
        } else if (o instanceof Iterator<?> && o instanceof Resettable) {
          while (((Iterator<?>) o).hasNext()) {
            size++;
            ((Iterator<?>) o).next();
          }
          ((Resettable) o).reset();
        } else {
          size++;
        }
      }
    }
    return size;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("MultiCollectionIterator.remove()");
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  public int getSkip() {
    return skip;
  }

  public void setSkip(final int skip) {
    this.skip = skip;
  }

  public void setAutoConvertToRecord(final boolean autoConvert2Record) {
    this.autoConvert2Record = autoConvert2Record;
  }

  public boolean isAutoConvertToRecord() {
    return autoConvert2Record;
  }

  @Override
  public boolean supportsFastContains() {
    final int totSources = sources.size();
    for (int i = 0; i < totSources; ++i) {
      final Object o = sources.get(i);

      if (o != null) {
        if (o instanceof Set<?> || o instanceof RidBag) {
          // OK
        } else if (o instanceof LazyWrapperIterator) {
          if (!((LazyWrapperIterator) o).canUseMultiValueDirectly()) {
            return false;
          }
        } else {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public boolean contains(final Object value) {
    final int totSources = sources.size();
    for (int i = 0; i < totSources; ++i) {
      Object o = sources.get(i);

      if (o != null) {
        if (o instanceof LazyWrapperIterator) {
          o = ((LazyWrapperIterator) o).getMultiValue();
        }

        if (o instanceof Collection<?>) {
          if (((Collection<?>) o).contains(value)) {
            return true;
          }
        } else if (o instanceof RidBag) {
          if (((RidBag) o).contains((Identifiable) value)) {
            return true;
          }
        }
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  protected boolean getNextPartial() {
    if (sourcesIterator != null) {
      while (sourcesIterator.hasNext()) {
        Object next = sourcesIterator.next();
        if (next != null) {

          if (!(next instanceof EntityImpl) && next instanceof Iterable<?>) {
            next = ((Iterable) next).iterator();
          }

          if (next instanceof Iterator<?>) {
            if (next instanceof Resettable) {
              ((Resettable) next).reset();
            }

            if (((Iterator<T>) next).hasNext()) {
              partialIterator = (Iterator<T>) next;
              return true;
            }
          } else if (next instanceof Collection<?>) {
            if (!((Collection<T>) next).isEmpty()) {
              partialIterator = ((Collection<T>) next).iterator();
              return true;
            }
          } else if (next.getClass().isArray()) {
            final int arraySize = Array.getLength(next);
            if (arraySize > 0) {
              if (arraySize == 1) {
                partialIterator = new IterableObject<T>((T) Array.get(next, 0));
              } else {
                partialIterator = (Iterator<T>) MultiValue.getMultiValueIterator(next);
              }
              return true;
            }
          } else {
            partialIterator = new IterableObject<T>((T) next);
            return true;
          }
        }
      }
    }

    return false;
  }

  public boolean isEmbedded() {
    return embedded;
  }

  public MultiCollectionIterator<T> setEmbedded(final boolean embedded) {
    this.embedded = embedded;
    return this;
  }

  @Override
  public String toString() {
    return "[" + size() + "]";
  }
}
